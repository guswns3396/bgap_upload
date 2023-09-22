import re

import numpy as np
import pdfquery
import pandas as pd
from pyquery import PyQuery

from . import RedcapUploader, RedcapUploaderError


class Item:
    def __init__(self, pg, x0, y1, txt):
        self.pg = int(pg)
        self.x0 = float(x0)
        self.y1 = float(y1)
        self.txt = txt.strip()


class KsadsUploader(RedcapUploader):
    def __init__(self, reports, template_path, api_url, token,
                 log_path, date_fields=None, uploaded_status=None,
                 skip_complete=True):
        self._reports = reports
        self._api_url = api_url
        self._token = token
        self._log_path = log_path
        self._template_path = template_path
        if date_fields is None:
            self._date_fields = []
        else:
            self._date_fields = date_fields

        super().__init__()

        if not uploaded_status:
            self._uploaded_status = self.UNVERIFIED
        else:
            self._uploaded_status = uploaded_status
        self._skip_complete = skip_complete


    def parse_data(self, diag_els, info_els, template):
        """
        diag_items: list of Items containing diagnosis data
        info_items: list of Items containing user information data
        template: CSV that contains REDCap variable name and field label

        Maps extracted data from PDF to corresponding REDCap variable

        Returns: dict that maps REDCap variable name to corresponding value extracted from PDF
        """
        redcap_vals = {}

        # get all xs
        diag_xs = sorted(set(x.x0 for x in diag_els))
        info_xs = sorted(set(x.x0 for x in info_els))

        # verify formatting
        if len(info_xs) != 5:
            raise ValueError(
                f"Expected 5 x0 values for info elements, got {len(info_xs)}"
            )
        if len(info_els) != 11:
            raise ValueError(
                f"Expected 11 elements for info elements, got {len(info_els)}"
            )

        # parse information data (id, event, date)
        subj, event = info_els[-4].txt.split('_')
        event = 'year_' + event + '_arm_1'
        redcap_vals[self.id_field()] = subj
        redcap_vals[self.event_field()] = event
        date_field = template[template['Variable / Field Name'].str.contains('date')]['Variable / Field Name'].values[0]
        redcap_vals[date_field] = info_els[1].txt

        # parse diagnosis data
        df = pd.DataFrame([[item.pg, item.x0, item.y1, item.txt] for item in diag_els],
                          columns=['pg', 'x0', 'y1', 'txt'])

        # get indices of element type's x0 in diag_xs
        def get_idx(df):
            time_x = 0
            no_diag_x = time_x + 1 if df['txt'].str.contains('No diagnosis').any() else 0
            dis_type_x = no_diag_x + 1 if df['txt'].str.contains('No diagnosis').sum() < 2 else 0
            diag_x = dis_type_x + 1 if dis_type_x else 0
            symp_x = diag_x + 1 if diag_x else 0
            suicid_symp_x = symp_x + 1 if df['txt'].str.contains('Suicidality').any() else 0
            desc_x = suicid_symp_x + 1 if suicid_symp_x else 0
            casa_x = desc_x + 1 if desc_x else 0
            comments_x = casa_x + 1 if casa_x else 0

            xs = pd.Series(
                [time_x, no_diag_x, dis_type_x, diag_x, symp_x, suicid_symp_x, desc_x, casa_x, comments_x],
                index="time_x, no_diag_x, dis_type_x, diag_x, symp_x, suicid_symp_x, desc_x, casa_x, comments_x".split(
                    ', ')
            )

            xs = xs.replace(0, np.nan)
            xs.loc['time_x'] = 0

            return xs[~xs.isna()]

        xs = get_idx(df)

        # verify xs match length of diag_xs
        if not len(xs) == len(diag_xs):
            raise ValueError(
                f"Expected lengh of xs to match length of diagnosis x0 found\n" +
                f"length of xs: {len(xs)}\n" +
                f"length of diagnosis x0s: {len(diag_xs)}"
            )

        suicid_fields = []
        for i, row in df.iterrows():
            txt = row['txt']

            # get item type
            for item_type, idx in xs.items():
                if diag_xs[int(idx)] == row['x0']:
                    break
            # get time
            if item_type == 'time_x':
                time = txt[:-len(' Diagnosis')]
            elif item_type == 'no_diag_x':
                continue
            elif item_type == 'dis_type_x':
                continue
            # items associated with columns
            elif item_type == 'diag_x' or item_type == 'symp_x':
                # replace present (occurs after comma or code) with current
                txt = re.sub(r', present', ', Current', txt, flags=re.IGNORECASE)
                txt = re.sub(r'\) present', ') Current', txt, flags=re.IGNORECASE)

                # replace special characters
                spec_char = {
                    b'\xef\xac\x81': 'fi'
                }
                for k in spec_char.keys():
                    txt = txt.replace(k.decode('utf-8'), spec_char[k])

                # if diagnosis
                if item_type == 'diag_x':
                    # skip if Suicidality
                    if 'Suicidality' in txt:
                        continue
                    # by space if disorder (only extract code & remission & time)
                    time = re.search(r"(\bCurrent)|(\bPast)", txt, re.IGNORECASE).group() if re.search(r"(\bCurrent)|(\bPast)", txt, re.IGNORECASE) else time
                    code = re.search(r"F\d+[.]\d+", txt).group()
                    remission = re.search(r'(partial remission)', txt, re.IGNORECASE)
                    # find field label with time, code, remission
                    ind = template['Field Label'].str.contains(r'\b' + time)
                    ind &= template['Field Label'].str.contains(code, regex=False)
                    ind &= template['Field Label'].str.contains('partial remission', case=False, regex=False) \
                        if remission \
                        else ~template['Field Label'].str.contains('partial remission', case=False, regex=False)
                # if symptom
                elif item_type == 'symp_x':
                    # by , if symptoms
                    tokens = txt.split(',')
                    # extract symptom, time
                    symp = tokens[0]
                    time_matches = re.findall(r"(\bCurrent)|(\bPast)", txt, flags=re.IGNORECASE)
                    # verify at most 1 time value
                    if len(time_matches) > 1:
                        raise ValueError(
                            f"Expected at most 1 value of time from symptom\n" +
                            f"{time_matches}"
                        )
                    # if 1 found, use that as time
                    elif len(time_matches) == 1:
                        time = time_matches[0][0] if time_matches[0][0] else time_matches[0][1]
                    # if 0 found use time from time_x
                    time = time.strip()
                    # find field label with symptom & time
                    ind = template['Field Label'].str.contains(symp, case=False, regex=False)
                    ind &= template['Field Label'].str.contains(r'\b' + time)
                else:
                    pass

                # if multiple possible matches
                if ind.sum() > 1:
                    raise ValueError(
                        "Field should have at most 1 match\n" +
                        f"{txt}" +
                        ', '.join(template.loc[ind]['Field Label'].values)
                    )
                # no matches => continue without mapping
                elif ind.sum() == 0:
                    continue

                # add to df for mapping (value = 1)
                col = template.loc[ind]['Variable / Field Name'].values[0]
                if col in redcap_vals:
                    raise ValueError(
                        f"Field Name already exists: {col}"
                    )
                redcap_vals[col] = 1
            elif item_type == 'suicid_symp_x':
                # just header
                if txt == 'Symptom':
                    continue

                # replace : with -
                txt = txt.replace(':', ' -').replace('\n', ' ')
                # match txt with field
                ind = template['Field Label'].str.contains(txt, case=False, regex=False)

                # only 1 match
                if ind.sum() != 1:
                    raise ValueError(
                        "Field should have at most 1 match\n" +
                        f"{txt}" +
                        ', '.join(template.loc[ind]['Field Label'].values)
                    )

                # store suicide field for completion at next iterations
                suicid_field = template.loc[ind]['Variable / Field Name'].values[0]
                redcap_vals[suicid_field] = []
                suicid_fields.append(suicid_field)
            elif item_type == 'desc_x':
                # just header
                if txt == 'Description':
                    continue

                # get description
                redcap_vals[suicid_field].append("Description: " + txt.replace('\n', ' '))
            elif item_type == 'casa_x':
                # just header
                if txt == 'C-\nCASA\nCode':
                    continue

                # get casa code
                redcap_vals[suicid_field].append('C-CASA Code: ' + txt.replace('\n', ' '))
            elif item_type == 'comments_x':
                # just header
                if txt == 'Patient\nComments':
                    continue

                # get patient comments
                redcap_vals[suicid_field].append('Patient Comments: ' + txt.replace('\n', ' '))
            else:
                raise ValueError(
                    f"Invalid item_type: {item_type}"
                )
        # turn suicidality fields from list to single string
        for suicid_field in suicid_fields:
            redcap_vals[suicid_field] = ' | '.join(redcap_vals[suicid_field])

        return redcap_vals

    @staticmethod
    def parse_info_elements(doc):
        """
        Parses PDF document to only get user information elements

        doc: PDF document

        returns: LTTBH elements of user information
        """
        # get current diagnosis element
        curr_diag_el = doc.find('LTTextBoxHorizontal').filter(
            lambda i, this: PyQuery(this).text() == 'Current Diagnosis'
        )
        # get user information element
        user_info_el = doc.find('LTTextBoxHorizontal').filter(
            lambda i, this: PyQuery(this).text() == 'User Information'
        )

        # filter to only get information  elements
        def filter_info_el(i, this, curr_diag_el=curr_diag_el, user_info_el=user_info_el):
            parents = PyQuery(this).parents()
            x0 = PyQuery(this).attr('x0')
            y1 = PyQuery(this).attr('y1')

            # get page of element
            for ind in range(len(parents)):
                pg = parents.eq(ind).attr('page_index')
                if pg is not None:
                    break

            # if first page => get btw user information & current diagnosis
            if pg == '0':
                prop = float(x0) >= float(user_info_el.attr('x0'))
                prop = prop and float(y1) <= float(user_info_el.attr('y1'))
                prop = prop and float(y1) > float(curr_diag_el.attr('y1'))
            # if not first page => filter out
            else:
                prop = False
            return prop

        info_elements = doc.find('LTTextBoxHorizontal').filter(filter_info_el)
        
        return info_elements

    @staticmethod
    def parse_diag_elements(doc):
        """
        Parses PDF document to only get diagnosis elements

        doc: PDF document

        returns: LTTBH elements of diagnoses
        """
        # get current diagnosis element
        curr_diag_el = doc.find('LTTextBoxHorizontal').filter(
            lambda i, this: PyQuery(this).text() == 'Current Diagnosis'
        )
        # get link element
        link_el = doc.find('LTTextBoxHorizontal').filter(
            lambda i, this: 'https:' in PyQuery(this).text()
        )
        # get CPC element
        cpc_el = doc.find('LTTextBoxHorizontal').filter(
            lambda i, this: PyQuery(this).text().startswith('CPC')
        )

        # filter to only get diagnosis elements
        def filter_diag_el(i, this, curr_diag_el=curr_diag_el, link_el=link_el, cpc_el=cpc_el):
            parents = PyQuery(this).parents()
            x0 = PyQuery(this).attr('x0')
            y1 = PyQuery(this).attr('y1')

            # get page of element
            for ind in range(len(parents)):
                pg = parents.eq(ind).attr('page_index')
                if pg is not None:
                    break

            # if first page => get btw current diag & link
            if pg == '0':
                prop = float(x0) >= float(curr_diag_el.attr('x0'))
                prop = prop and float(y1) <= float(curr_diag_el.attr('y1'))
                prop = prop and float(y1) > float(link_el.attr('y1'))
            # if not first page => get btw cpc & link
            else:
                prop = float(x0) >= float(curr_diag_el.attr('x0'))
                prop = prop and float(y1) < float(cpc_el.attr('y1'))
                prop = prop and float(y1) > float(link_el.attr('y1'))
            # exclude copyright
            prop = prop and not PyQuery(this).text().startswith("Copyright")
            return prop

        diag_elements = doc.find('LTTextBoxHorizontal').filter(filter_diag_el)

        return diag_elements
    
    @staticmethod
    def sort_el_coord(elements):
        """
        Groups the elements into a Item objects
        Returns the list of elements sorted by page, y1, x0

        elements: LTTBH elements

        returns: sorted list of Items
        """
        items = []
        for i in range(len(elements)):
            el = elements.eq(i)
            x0 = float(el.attr('x0'))
            y1 = float(el.attr('y1'))
            pg = int(el.parents('LTPage').attr('page_index'))
            txt = el.text()
            item = Item(pg, x0, y1, txt)
            items.append(item)
        return sorted(items, key=lambda k: (k.pg, -1 * k.y1, k.x0))

    def pull(self):
        errors = []
        pulled_data = []

        # Iterate over timepoints
        for (subj, event), report in self._reports.items():
            # load pdf
            pdf = pdfquery.PDFQuery(report.report_path)
            pdf.load()
            # convert the pdf to XML
            pdf.tree.write('test.xml', pretty_print=True)
            doc = pdf.pq
            # parse pdf & sort elements
            diag_els = self.sort_el_coord(self.parse_diag_elements(doc))
            info_els = self.sort_el_coord(self.parse_info_elements(doc))

            template = pd.read_csv(self._template_path)
            redcap_vals = self.parse_data(diag_els, info_els, template)

            # verify subj, event matches
            # TODO: verify youth vs parent
            pdf_subj, pdf_event = redcap_vals[self.id_field()], redcap_vals[self.event_field()]
            try:
                if pdf_subj != subj:
                        raise KsadsUploaderError(
                            f'Form subject ID {pdf_subj} does not '
                            f'match the provided subject ID.', subj_id=subj,
                            event=event, form_path=report.report_path
                        )
                if pdf_event != event:
                    raise KsadsUploaderError(
                        f'Form timepoint {pdf_event} does not '
                        f'match the provided timepoint.', subj_id=subj,
                        event=event, form_path=report.report_path
                    )
            except KsadsUploaderError as err:
                errors.append(err)
                continue

            # skip if already complete
            skip = False
            for field in redcap_vals:
                if field == self.id_field() or field == self.event_field():
                    continue
                elif (self._skip_complete and
                        self.is_complete(subj, event, field)):
                    skip = True
                    break
                else:
                    completed_field = self.completed_field(field)
                    redcap_vals[completed_field] = self._uploaded_status
                    break
            if skip:
                continue

            # verify all cols exist
            bad_redcap_fields = []
            for field in redcap_vals.keys():
                if field == self.id_field() or field == self.event_field():
                    continue
                elif field not in self.field_names():
                    bad_redcap_fields.append(field)
            if bad_redcap_fields:
                raise ValueError(
                    'These field(s) do not exist in the REDCap database:\n' +
                    ", ".join(bad_redcap_fields))

            if redcap_vals:
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
