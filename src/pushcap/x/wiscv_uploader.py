import csv

from . import RedcapUploader, RedcapUploaderError

class QInteractiveUploader(RedcapUploader):
    def __init__(self, reports, template_path, api_url, token, log_path,
                 info_mappings=None, date_fields=None, uploaded_status=None,
                 skip_complete=True):
        self._api_url = api_url
        self._token = token
        if date_fields is None:
            self._date_fields = []
        else:
            self._date_fields = date_fields

        super().__init__()

        self._reports = reports
        self._log_path = log_path
        self._info_rcmap = info_mappings
        if not uploaded_status:
            self._uploaded_status = self.UNVERIFIED
        else:
            self._uploaded_status = uploaded_status
        self._skip_complete = skip_complete

        self._rcmap = {}
        for group, test, col, field in qint_reader(template_path):
            if field:
                if field[0] != '[' or field[-1] != ']':
                    raise ValueError(
                            f'Template parse error at ({group}, {test}, '
                            f'{col}): expecting bracketed REDCap field.')
                self._rcmap[(group, test, col)] = field[1:-1]

        bad_redcap_fields = [field for field in self._rcmap.values()
                             if field not in self.field_names()]
        if bad_redcap_fields:
            raise ValueError(
                    'These field(s) do not exist in the REDCap database:\n' +
                    ", ".join(bad_redcap_fields))

    def pull(self):
        errors = []
        pulled_data = []
        for (subj_id, event), report in self._reports.items():
            print(f'Parsing {subj_id}, {event}...')
            redcap_vals = {}

            try:
                for group, test, col, val in qint_reader(report.report_path):
                    try:
                        redcap_field = self._rcmap[(group, test, col)]
                    except KeyError:
                        continue
                    if ( self._skip_complete and
                         self.is_complete(subj_id, event, redcap_field) ):
                        continue
                    else:
                        redcap_vals[redcap_field] = val
                        completed_field = self.completed_field(redcap_field)
                        redcap_vals[completed_field] = self._uploaded_status

                if (report.info_path is not None) and self._info_rcmap:
                    info_vals = report.extract_info(self._info_rcmap)
                    for redcap_field, val in info_vals.items():
                        if ( self._skip_complete and
                             self.is_complete(subj_id, event, redcap_field) ):
                            continue
                        else:
                            redcap_vals[redcap_field] = val
                            completed_field = self.completed_field(redcap_field)
                            redcap_vals[completed_field] = self._uploaded_status

            except RedcapUploaderError as err:
                errors.append(err)

            if redcap_vals:
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
                

def qint_reader(qint_path):
    qint_file = _read_unknown_encoding(qint_path)
    qint_lines = [l.strip('"') for l in qint_file.readlines()]
    qint_file.close()
    try:
        reader = csv.reader(qint_lines)
        while 1:
            # Group
            try:
                row = next(reader)
                while not row or not row[0]:
                    row = next(reader)
            except StopIteration:
                break

            group = row[0]
            if not group:
                raise QInteractiveUploaderError(
                        f'Parse error at line {",".join(row)}: '
                         'expecting first column here to be a group.',
                         form_path=qint_path)
            if next(reader):
                raise RedcapUploaderError(
                        f'Parse error at {group}: expecting next line to be '
                        f'blank.', form_path=qint_path)

            # Header row
            header_row = next(reader)
            if not header_row:
                raise RedcapUploaderError(
                        f'Parse error at {group}: no header row after '
                         'blank line.', form_path=qint_path)

            # Scores
            try:
                row = next(reader)
                while row:
                    for idx, col_name in enumerate(header_row[1:]):
                        if col_name:
                            yield (group, row[0], col_name, row[idx+1])
                    row = next(reader)
            except StopIteration:
                break
    finally:
        qint_file.close()


def _read_unknown_encoding(path, encodings=('utf16', 'utf8', 'Windows-1252')):
    for encoding in encodings:
        f = open(path, 'r', encoding=encoding)
        try:
            f.readlines()
            f.seek(0)
            return f
        except (UnicodeError, UnicodeDecodeError):
            f.close()
            continue
        except:
            f.close()
            raise

    raise QInteractiveUploaderError(
            'Could not read info file with provided encodings.', form_path=path)


class QInteractiveReport():
    def __init__(self, report_path, info_path=None):
        self.report_path = report_path
        self.info_path = info_path

    def extract_info(self, mappings):
        if self.info_path is None:
            raise QInteractiveUploaderError(
                    'Cannot request info without an info path.',
                    form_id='qinteractive', form_path=report_path)
        vals_dict = {}
        info_file = _read_unknown_encoding(self.info_path)
        try:
            lines = info_file.readlines()
        finally:
            info_file.close()

        for line in lines:
            split_vals = line.rstrip().split(': ', 1)
            if len(split_vals) == 2 and split_vals[0] in mappings:
                redcap_field = mappings[split_vals[0]][0]
                xlate_fn = mappings[split_vals[0]][1]
                vals_dict[redcap_field] = xlate_fn(split_vals[1])
        return vals_dict


class QInteractiveUploaderError(RedcapUploaderError):
    pass
