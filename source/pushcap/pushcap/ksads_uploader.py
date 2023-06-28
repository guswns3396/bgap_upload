import csv

from . import RedcapUploader, RedcapUploaderError

class KsadsUploader(RedcapUploader):
    def __init__(self, report_path, template_path, parse_id_fn, api_url, token,
                 log_path, date_fields=None, uploaded_status=None,
                 skip_complete=True):
        self._api_url = api_url
        self._token = token
        if date_fields is None:
            self._date_fields = []
        else:
            self._date_fields = date_fields

        super().__init__()

        self._report_path = report_path
        self._log_path = log_path
        if not uploaded_status:
            self._uploaded_status = self.UNVERIFIED
        else:
            self._uploaded_status = uploaded_status
        self._skip_complete = skip_complete

        self._parse_id = parse_id_fn
        with open(template_path, 'r') as template_file:
            self._rcmaps = {r['UserType']: {k: v for k, v in r.items()
                                            if v and (k != 'UserType')}
                            for r in csv.DictReader(template_file)}

        bad_redcap_fields = [field for rcmap in self._rcmaps.values()
                             for field in rcmap.values()
                             if field not in self.field_names()]
        if bad_redcap_fields:
            raise ValueError(
                    'These field(s) do not exist in the REDCap database:\n' +
                    ", ".join(bad_redcap_fields))

    def pull(self):
        errors = []
        pulled_data = []

        with open(self._report_path, 'r') as report_file:
            for scores in csv.DictReader(report_file):
                redcap_vals = {}
                # Get the correct rcmap for this type of report
                rcmap = self._rcmaps[scores['UserType']]
                redcap_vals = {rcvar: scores[measure]
                               for measure, rcvar in rcmap.items()}
                subj_id, event = self._parse_id(scores['PatientCode'])

                if self._skip_complete:
                    redcap_vals = {
                            rcvar: value for rcvar, value in redcap_vals.items()
                            if not self.is_complete(subj_id, event, rcvar)}

                if redcap_vals:
                    redcap_fields = list(redcap_vals.keys())
                    for redcap_field in redcap_fields:
                        completed_field = self.completed_field(redcap_field)
                        redcap_vals[completed_field] = self._uploaded_status
                    redcap_vals[self.id_field()] = subj_id
                    redcap_vals[self.event_field()] = event
                    pulled_data.append(redcap_vals)

        return pulled_data, errors

    def api_url(self):
        return self._api_url

    def token(self):
        return self._token

    def log_path(self):
        return self._log_path

    def date_fields(self):
        return self._date_fields

    def change_log_path(self, new_log_path):
        self._log_path = new_log_path
                
class KsadsUploaderError(RedcapUploaderError):
    pass
