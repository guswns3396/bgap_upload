import csv
import re

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

    def pull_helper(self, lttbhs, template):
        """
        lttbhs: All the LTTextBoxHorizontal elements from the PDF file.
                Contains all the necessary text data
        template: CSV that contains REDCap variable name and field label

         Maps extracted data from PDF to corresponding REDCap variable

         Returns: dict that maps REDCap variable name to corresponding value extracted from PDF
        """

        mapping = {}
        for i in range(len(lttbhs)):
            lttbh = lttbhs.eq(i)
            txt = lttbh.text().strip()
            x0 = float(lttbh.attr('x0'))

            # x0 = 73.62 => 253.89
            # x0 = 384.863 => 478.309

            if txt == 'User Information':
                continue
            # User Information type
            if x0 in [73.62, 384.863]:
                info_type = txt
            # user info val
            elif x0 in[253.89, 478.309]:
                info_val = txt
                if info_type == 'Patient Id':
                    pat_id, year = info_val.split('_')
                    mapping[self.id_field()] = pat_id
                # TODO: finish user info
            # items associated with columns
            elif x0 in [93.486, 99.373]:
                # replace present (occurs after comma or code) with current
                txt = re.sub(r', present', ', Current', txt, flags=re.IGNORECASE)
                txt = re.sub(r'\) present', ') Current', txt, flags=re.IGNORECASE)

                # replace special characters
                spec_char = {
                    b'\xef\xac\x81': 'fi'
                }
                for k in spec_char.keys():
                    txt = txt.replace(k.decode('utf-8'), spec_char[k])

                # separate into tokens
                if x0 == 93.486:
                    # skip if Suicidality
                    if 'Suicidality' in txt:
                        continue

                    # by space if disorder (only extract code & remission & time)
                    time = re.search(r"(\bCurrent)|(\bPast)", txt, re.IGNORECASE).group()
                    code = re.search(r"\(.+?\)", txt).group()
                    remission = re.search(r'(partial remission)', txt, re.IGNORECASE)

                    # find field label with time, code, remission
                    ind = template['Field Label'].str.contains(r'\b' + time)
                    ind &= template['Field Label'].str.contains(code, regex=False)
                    ind &= template['Field Label'].str.contains('partial remission', case=False, regex=False) \
                        if remission \
                        else ~template['Field Label'].str.contains('partial remission', case=False, regex=False)

                    # make sure only 1 field corresponds
                    if ind.sum() != 1:
                        raise ValueError(
                            "Field should have exactly 1 match\n" +
                            f"{txt}: {code} | {remission.group()} | {time}\n" +
                            ', '.join(template.loc[ind]['Field Label'].values)
                        )

                    # add to df for mapping (value = txt)
                    col = template.loc[ind]['Variable / Field Name'].values[0]
                    if col in mapping:
                        raise ValueError(
                            f"Field Name already exists: {col}"
                        )
                    mapping[col] = txt

                elif x0 == 99.373:
                    # by , if symptoms
                    tokens = txt.split(',')

                    # verify exactly 2 tokens (symptom, time)
                    if len(tokens) != 2:
                        raise ValueError(
                            "Expected 2 tokens from symptom item\n" +
                            " | ".join(tokens)
                        )

                    # extract symptom, time
                    symp, time = tokens
                    time = time.strip()
                    # find field label with symptom & time
                    ind = template['Field Label'].str.contains(symp, case=False, regex=False)
                    ind &= template['Field Label'].str.contains(r'\b' + time)

                    # make sure only 1 field corresponds
                    if ind.sum() != 1:
                        raise ValueError(
                            "Field should have exactly 1 match\n" +
                            f"{txt}: {symp} | {time}\n" +
                            ', '.join(template.loc[ind]['Field Label'].values)
                        )

                    # add to df for mapping (value = 1)
                    col = template.loc[ind]['Variable / Field Name'].values[0]
                    if col in mapping:
                        raise ValueError(
                            f"Field Name already exists: {col}"
                        )
                    mapping[col] = 1

            else:
                continue

        return mapping

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


class KsadsReport:
    def __init__(self, report_path):
        self.report_path = report_path


class KsadsUploaderError(RedcapUploaderError):
    pass
