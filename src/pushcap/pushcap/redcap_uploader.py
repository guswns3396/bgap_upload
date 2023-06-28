from abc import ABC, abstractmethod
import json

from dateutil.parser import parse as dateparse
import requests

class RedcapUploader(ABC):
    """Abstract class defining uploaders that parse & upload scores to REDCap.

    A REDCap Uploader is responsible for parsing scores files (for assessments,
    cognitive tasks, etc.), mapping those scores to values in a REDCap database,
    and pushing those scores up to that database. Each type of scoring file
    has its own uploader that must subclass this abstract class, implementing a
    `pull` method that returns a list of dictionaries of REDCap fields -> scores
    from a set of scoring files & typically a template that it receives in its
    constructor. It also must implement some methods returning details about its
    REDCap database: the API URL, the REDCap token, where to log the results,
    and any fields that should be formatted as dates.

    This abstract class provides functions for interacting with the REDCap
    database, providing things like the REDCap ID/event/standard fields,
    longitudinal events, and relationships between fields and forms. It handles
    the details of all of the REDCap requests, including pushing the scores.

    Constants:
      INCOMPLETE: Default 'incomplete' value for a form's completed field.
      UNVERIFIED: Default 'unverified' value for a form's completed field.
      COMPLETE: Default 'complete' value for a form's completed field.
      


    Private members:
      _field_forms: Dictionary mapping REDCap fields to their form names.
      _id_field: Name of the special identifier field in REDCap.
      _events: List of REDCap events for the project.
      _field_names: List of all fields in the project.
      _form_complete: Completed status for all forms across all subjects, s.t.
          self._form_complete[id][event][form] = completed status for that form
    """

    # REDCap default values for the `complete` fields
    INCOMPLETE = '0'
    UNVERIFIED = '1'
    COMPLETE = '2'

    def __init__(self):
        """Default constructor for REDCap uploaders.

        This constructor requires token and api_url to be implemented before
        being called by a subclass. Thus, the constructor for a form uploader
        would look something like this:

        def __init__(self, arg1, arg2, ..., argn):

            # Do some stuff here and implement the token & api_url methods

            super().__init__()  # Will initialize a bunch of REDCap methods

            # Do some more stuff here, now with access to the REDCap methods
            # defined in this abstract class.

        This constructor pulls the metadata for the REDCap database defined by
        the api_url and token, maps all REDCap fields to forms, notes the
        completed status for all records, and notes the project's events.
        """
        # Get field/form metadata
        fields = self._redcap_request('metadata')
        self._field_forms = {f['field_name']: f['form_name'] for f in fields}

        # Also add ID, events, and "complete" fields to our forms dict:
        for form in set(self._field_forms.values()):
            self._field_forms[f'{form}_complete'] = form
        self._id_field = fields[0]['field_name']
        self._events = [e['unique_event_name']
                        for e in self._redcap_request('event')]
        self._field_names = [self.event_field()] + [self._field_forms.keys()]

        # Get completed status for all records and forms
        params = {'fields[0]': self.id_field()}
        params.update({f'fields[{idx+1}]': f'{f}_complete'
                      for idx, f in enumerate(set(self._field_forms.values()))})
        completed_statuses = self._redcap_request('record', params)

        # self._form_complete[id][event][form] = form completed status
        self._form_complete = {}
        for event_record in completed_statuses:
            if event_record['redcap_repeat_instance']:
                # Repeat instruments not currently supported
                continue
            record_id = event_record[self.id_field()]
            event = event_record[self.event_field()]
            if record_id not in self._form_complete.keys():
                self._form_complete[record_id] = {}
            self._form_complete[record_id][event] = {}
            for field, value in event_record.items():
                if field in self.standard_fields():
                    continue
                assert(field.endswith('_complete'))
                self._form_complete[record_id][event][field[:-9]] = value

    def _redcap_request(self, content, params={}):
        """Make an API request to this uploader's REDCap database.

        Forms & submits an API request to the uploader's database, taking care
        of the default token/format/api_url fields, then returning the content.

        Arguments:
          content: Type of content for the 'content' field, as defined in the
              REDCap API, e.g. metadata, record, event, etc.
          params: Dictionary with any specific parameters to pass for this
              request, e.g., returnContent, overwriteBehavior, etc., optional

        Returns: REDCap response, JSON-decoded into a list of dicts.

        Raises: REDCapUploaderError if the API call was not successful with
          details of why.
        """
        request_params = {'token': self.token(),
                          'format': 'json',
                          'content': content}
        request_params.update(params)
        response = requests.post(self.api_url(), data=request_params)
        if response.reason == 'OK':
            return json.loads(response.content)
        elif response.reason == 'Forbidden':
            raise RedcapUploaderError(
                    'API request rejected (Forbidden). This is most likely due '
                    'to not being connected to the Stanford VPN, but could '
                    'also be due to a bad/expired token.')
        else:
            raise RedcapUploaderError(
                    f'API request error: {response.status_code}: '
                    f'{response.reason}\n{response.content}')

    def id_field(self):
        """Returns the special identifier field (string) for this database."""
        return self._id_field

    def event_field(self):
        """Returns the standard event field (string) for this database."""
        return 'redcap_event_name'

    def standard_fields(self):
        """Returns a list of all "standard fields" for this database.

        Includes the identifier field, the event field, and fields for repeat
        instances/instruments. Useful for excluding the standard fields from
        all of the other form fields.
        """
        return (self.id_field(), self.event_field(), 'redcap_repeat_instance',
                'redcap_repeat_instrument')

    def events(self):
        """Returns a list of all the REDCap events for this database."""
        return self._events

    def field_names(self):
        """Returns a list of all REDCap fields for this database."""
        return list(self._field_forms.keys())

    def field_form(self, field):
        """Returns the name of the form that houses the given `field`."""
        return self._field_forms[field]

    def completed_field(self, field):
        """Returns the completed status of the form for the given `field`."""
        return f'{self.field_form(field)}_complete'

    def record_ids(self):
        """Returns a list of all record identifiers for this database."""
        return list(set(self._form_complete.keys()))

    def is_complete(self, record_id, event, field):
        """Returns the 'complete' status for a given ID/event/field.

        Arguments:
          record_id: The record identifier for the requested status.
          event: The longitudinal event for the requested status.
          field: The field name for the requested status.

        Returns: True if the record_id / event / field combination exists and
          the status of the associated form's completion status is complete.

        Raises: RedcapUploaderError if the record_id is not in the REDCap
          database.
        """
        if record_id not in self.record_ids():
            raise RedcapUploaderError(f'Record ID {record_id} not found in '
                                       'REDCap database.')
        form = self.field_form(field)
        return (event in self._form_complete[record_id].keys() and
                form in self._form_complete[record_id][event].keys() and
                self._form_complete[record_id][event][form] == self.COMPLETE)

    @abstractmethod
    def pull(self):
        """Pulls & parses the uploader's scores and map to their REDCap fields.

        Returns: A list of dictionaries mapping REDCap fields to score values.
          Each dictionary represents a single record_id-event pair (i.e.,
          a single visit consisting of multiple scores or assessments), and
          must have values for id_field and event_field (if appropriate). The
          other values in that dictionary map individual REDCap fields to the
          scores for that subject-event.
        """
        pass

    @abstractmethod
    def api_url(self):
        """Returns the API URL for this REDCap database."""
        pass

    @abstractmethod
    def token(self):
        """Returns the token for this REDCap database."""
        pass

    @abstractmethod
    def log_path(self):
        """Returns the log file path for this REDCap database."""
        pass

    @abstractmethod
    def date_fields(self):
        """Returns the field names of any date fields for this REDCap database.

        These date fields must be reformatted to be imported into REDCap.
        """
        pass

    def _reformat_dates(self, data):
        """Reformats all dates in a `pull` dict list for importing into REDCap.

        Arguments:
          data: Dictionary mapping REDCap fields to score values.

        Returns: A matching `pull` dict list but with reformatted dates.
        """
        reformatted_data = data.copy()

        reformat_date = lambda x: dateparse(x).strftime('%Y-%m-%d')

        for record in reformatted_data:
            for date_field in self.date_fields():
                try:
                    record[date_field] = reformat_date(record[date_field])
                except KeyError:
                    pass

        return reformatted_data

    def push(self):
        """Pushes a dictionary of score values up to this REDCap database.

        This method uses the `pull` method to generate a list of dictionaries of
        field-names -> score values, scans it for errors, reformats the dates to
        match REDCap specs, pushes the data to REDCap, writes the data to the
        log file, and returns the subject-events that it pushed to REDCap.

        Returns: List of (subject_id, event_string) tuples included in the
          REDCap push request.

        Raises: RedcapUploaderError if there is an error in the `pull` data.
        """
        data, errors = self.pull()
        if not data:
            return [], [], errors

        clean_data, push_errors = self._check_push_data(data)
        errors += push_errors

        clean_data = self._reformat_dates(clean_data)

        log_path = self.log_path()
        data_str = json.dumps(clean_data)
        if log_path:
            log_path.write_text(data_str)

        params = {'returnContent': 'ids',
                  'overwriteBehavior': 'overwrite',
                  'data': data_str}

        subjs_tps = [(d[self.id_field()], d[self.event_field()])
                     for d in clean_data]
        subjs_tps.sort(key=lambda x: (x[1], x[0]))
        
        response = self._redcap_request('record', params)
        return subjs_tps, response, errors

    def _check_push_data(self, data):
        """Checks the list of dicts from a `pull` request for common errors.

        Arguments:
          data: List of dictionaries from a `pull` request.

        Raises: RedcapUploaderError if:
          - Any subj-tp dictionaries don't include a value for the ID field
          - Any subj-tp dictionaries don't include a value for the event field
          - Any record has a subject ID not in REDCap
          - Any record has an event not in REDCap
          - Any record has fields not in REDCap

        The error string lists all records with issues.
        """
        all_ids = self.record_ids()
        all_events = self.events()
        all_fields = [self.event_field()] + self.field_names()

        clean_data = []
        errors = []

        for idx, record in enumerate(data):
            # Record ID checks
            try:
                record_id = record[self.id_field()]
            except KeyError:
                errors.append(RedcapUploaderError(
                        'No record ID field {self.id_field()}: {record}.'))
                continue
            else:
                if record_id not in all_ids:
                    errors.append(RedcapUploaderError(
                            f'Subject ID {record_id} not in REDCap: {record}',
                            subj_id=record_id))
                    continue

            # Event checks
            try:
                event = record[self.event_field()]
            except KeyError:
                errors.append(RedcapUploaderError(
                        f'No event field {self.event_field()}.',
                        subj_id=record_id))
                continue
            else:
                if event not in all_events:
                    errors.append(RedcapUploaderError(
                            f'Event {event} not in REDCap.', subj_id=record_id,
                            event=event))
                    continue

            # Field checks
            bad_fields = [f for f in record.keys() if f not in all_fields]
            if bad_fields:
                errors.append(RedcapUploaderError(
                        f'Fields {", ".join(bad_fields)} not in REDCap.',
                        subj_id=record_id, event=event))
                continue
        
            clean_data.append(record)

        return clean_data, errors


class RedcapUploaderError(Exception):
    """Generic error for this abstract class that can also be subclassed."""
    def __init__(self, err_msg, subj_id=None, event=None, form_id=None,
                 form_path=None):
        self.err_msg = err_msg
        self.subj_id = subj_id
        self.event = event
        self.form_id = form_id
        self.form_path = form_path

    def __str__(self):
        return (f'{self.err_msg}  (subject id: {self.subj_id}, '
                f'event: {self.event}, form: {self.form_id}, '
                f'path: {self.form_path})')
