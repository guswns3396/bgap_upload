from abc import ABC, abstractmethod
import requests
import json

class RedcapUploader(ABC):
    INCOMPLETE = '0'
    UNVERIFIED = '1'
    COMPLETE = '2'

    def __init__(self):
        """Requires token and api_url to be implemented."""
        # Get field/form metadata
        fields = self._redcap_request('metadata')
        self._forms = {f['field_name']: f['form_name'] for f in fields}
        # Also add "complete" fields to our forms dict:
        for form in set(self._forms.values()):
            self._forms[f'{form}_complete'] = form
        self._id_field = fields[0]['field_name']

        self._events = [e['unique_event_name']
                        for e in self._redcap_request('event')]

        self._field_names = [self.event_field()] + [self._forms.keys()]

        # Get completed status for all records and forms
        params = {'fields[0]': self.id_field(),
                  'fields[1]': self.event_field()}
        params.update({f'fields[{idx+2}]': f'{f}_complete'
                      for idx, f in enumerate(set(self._forms.values()))})
        completed_statuses = self._redcap_request('record', params)

        # self._form_complete[id][event][form] = form completed status
        self._form_complete = {}
        for event_record in completed_statuses:
            record_id = event_record[self.id_field()]
            event = event_record[self.event_field()]
            if record_id not in self._form_complete.keys():
                self._form_complete[record_id] = {}
            self._form_complete[record_id][event] = {}
            for field, value in event_record.items():
                if field in (self.id_field(), self.event_field()):
                    continue
                assert(field.endswith('_complete'))
                self._form_complete[record_id][event][field[:-9]] = value

    def _redcap_request(self, content, params={}):
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
        return self._id_field

    def event_field(self):
        return 'redcap_event_name'

    def events(self):
        return self._events

    def field_names(self):
        return list(self._forms.keys())

    def field_form(self, field):
        return self._forms[field]

    def completed_field(self, field):
        return f'{self.field_form(field)}_complete'

    def record_ids(self):
        return list(set(self._form_complete.keys()))

    def is_complete(self, record_id, event, field):
        if record_id not in self.record_ids():
            raise RedcapUploaderError(f'Record ID {record_id} not found in '
                                       'REDCap database.')
        form = self.field_form(field)
        return (event in self._form_complete[record_id].keys() and
                form in self._form_complete[record_id][event].keys() and
                self._form_complete[record_id][event][form] == self.COMPLETE)

    @abstractmethod
    def pull(self): pass

    @abstractmethod
    def api_url(self): pass

    @abstractmethod
    def token(self): pass

    @abstractmethod
    def log_path(self): pass

    def push(self):
        data = self.pull()
        if not data:
            return []

        err_string, errs = self._check_push_data(data)
        if err_string:
            raise RedcapUploaderError(f'Bad push data:\n\n{err_string}')

        log_path = self.log_path()
        data_str = json.dumps(data)
        if log_path:
            log_path.write_text(data_str)

        params = {'returnContent': 'ids',
                  'overwriteBehavior': 'overwrite',
                  'data': data_str}

        subjs_tps = [(d[self.id_field()], d[self.event_field()]) for d in data]
        subjs_tps.sort(key=lambda x: (x[1], x[0]))
        
        response = self._redcap_request('record', params)
        return subjs_tps

    def _check_push_data(self, data):
        all_ids = self.record_ids()
        all_events = self.events()
        all_fields = [self.event_field()] + self.field_names()

        no_id_field = []
        no_event_field = []
        bad_id = []
        bad_event = []
        bad_field = []

        for idx, record in enumerate(data):
            # Record ID checks
            try:
                record_id = record[self.id_field()]
            except KeyError:
                no_id_field.append(idx)
            else:
                if record_id not in all_ids:
                    bad_id.append((idx, record_id))

            # Event checks
            try:
                event = record[self.event_field()]
            except KeyError:
                no_event_field.append(idx)
            else:
                if event not in all_events:
                    bad_event.append((idx, event))

            # Field checks
            bad_fields = [f for f in record.keys() if f not in all_fields]
            if bad_fields:
                bad_field.append((idx, ', '.join(bad_fields)))
        
        error_str = ''
        if no_id_field:
            error_str += 'Missing ID field: '
            error_str += ", ".join([str(r) for r in no_id_field]) + '\n'
        if no_event_field:
            error_str += 'Missing event field: '
            error_str += ", ".join([str(r) for r in no_event_field]) + '\n'
        if bad_id:
            error_str += 'IDs not in REDCap: '
            error_str += ", ".join([r[1] for r in bad_id]) + '\n'
        if bad_event:
            error_str += 'Events not in REDCap: '
            error_str += ", ".join([r[1] for r in bad_event]) + '\n'
        if bad_field:
            error_str += 'Fields not in REDCap: \n'
            for r in bad_field:
                error_str += f'Record {r[0]}: {r[1]}\n'

        return error_str, (no_id_field, no_event_field, bad_id, bad_event,
                           bad_field)


class RedcapUploaderError(Exception):
    pass
