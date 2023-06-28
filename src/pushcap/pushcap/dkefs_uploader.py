import re

from . import RedcapUploader, RedcapUploaderError

class DkefsUploader(RedcapUploader):
    def __init__(self, reports, template_path, api_url, token, log_path, 
                 date_fields=None, skip_complete=True, uploaded_status=None):

        self._reports = reports
        self._api_url = api_url
        self._token = token
        self._log_path = log_path
        if date_fields is None:
            self._date_fields = []
        else:
            self._date_fields = date_fields

        super().__init__()

        self._rc_map = self.parse_template(template_path)

        if not uploaded_status:
            self._uploaded_status = self.UNVERIFIED
        else:
            self._uploaded_status = uploaded_status
        self._skip_complete = skip_complete

    def pull(self):
        pulled_data = []
        errors = []

        # Iterate over timepoints
        for (subj, event) in self._reports.keys():

            redcap_vals = {}

            # Iterate over score files for that timepoint
            for (report_path, subform) in self._reports[(subj, event)].reports:
                try:
                    form_vals = report_path.read_text().strip().split(",")
                    form_fields = self._rc_map[subform]

                    if len(self._rc_map[subform]) != len(form_vals):
                        raise DkefsUploaderError(
                                f'Number of fields in {subform} template file'
                                f' ({len(form_fields)}) does not match the '
                                f'score file ({len(form_vals)}).',
                                subj_id=subj, event=event,
                                form_id=subform, form_path=report_path)

                    # Iterate over the fields for that type of score file
                    for col_idx, field in enumerate(form_fields):
                        if not field:
                            continue
                        elif field[0] == '+':
                            increment_update = True
                            field = field[1:]
                        else:
                            increment_update = False

                        if ( self._skip_complete and
                             self.is_complete(subj, event, field) ):
                            continue

                        value = form_vals[col_idx]

                        if increment_update:
                            if value.strip():
                                value = int(value)
                            else:
                                value = 0
                            try:
                                redcap_vals[field] = redcap_vals[field] + value
                            except KeyError:
                                redcap_vals[field] = value
                        else:
                            redcap_vals[field] = value

                        if field != self.id_field():
                            completed_field = self.completed_field(field)
                            redcap_vals[completed_field] = self._uploaded_status

                    try:
                        form_subj_id = redcap_vals[self.id_field()]
                    except KeyError:
                        raise DkefsUploaderError(
                                f'No subject ID within the form scoring file.',
                                subj_id=subj, event=event,
                                form_id=subform, form_path=report_path)

                    if not re.match(f'{subj}(?:_[12])?', form_subj_id):
                        raise DkefsUploaderError(
                                f'Form subject ID {form_subj_id} does not '
                                f'match the provided subject ID.', subj_id=subj,
                                event=event, form_id=subform,
                                form_path=report_path)
                except DkefsUploaderError as err:
                    errors.append(err)
                    continue

            if redcap_vals:
                redcap_vals[self.id_field()] = subj
                redcap_vals[self.event_field()] = event
                pulled_data.append(redcap_vals)

        return pulled_data, errors

    def parse_template(self, template_path):
        rc_map = {}
        with open(template_path, 'r') as template_file:
            while 1:
                try:
                    row = next(template_file).strip()
                except StopIteration:
                    break

                if row:
                    # First non-empty row should be the instrument abbrev/name
                    columns = row.split(',')
                    if len(columns) != 2:
                        raise ValueError(
                                f'Expecting instrument abbreviation and name '
                                f'after a blank line in the template file '
                                f'(got {row}).')
                    instr = columns[0]
                    try:
                        rc_map[instr] = next(template_file).strip().split(',')
                    except StopIteration:
                        raise ValueError('No REDCap mappings in the template '
                                         f'file for {instr}.')
                    if len(rc_map[instr]) == 0:
                        raise ValueError('No REDCap mappings in the template '
                                         f'file for {instr}.')

        return rc_map

    def api_url(self):
        return self._api_url

    def token(self):
        return self._token

    def log_path(self):
        return self._log_path

    def date_fields(self):
        return self._date_fields


class DkefsReport:
    def __init__(self):
        self.reports = []

    def add_report(self, report_path, subform):
        self.reports.append((report_path, subform))

    def forms(self):
        return set(subform for (_, subform) in self.reports)

    def has_form(self, form):
        for _, f in self.reports:
            if form == f:
                return True
        else:
            return False


class DkefsUploaderError(RedcapUploaderError):
    pass
