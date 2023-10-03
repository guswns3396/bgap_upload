import re
import os

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
        for info_el in info_els:
            txt = info_el.txt
            if re.match('\d+_\d', txt):
                subj, event = txt.split('_')
                event = 'year_' + event + '_arm_1'
                redcap_vals[self.id_field()] = subj
                redcap_vals[self.event_field()] = event
            elif re.match('\d\d?/\d\d?/\d\d\d?\d?', txt):
                date_field = \
                    template[template['Variable / Field Name'].str.contains('date')]['Variable / Field Name'].values[0]
                redcap_vals[date_field] = txt

        # parse diagnosis data
        df = pd.DataFrame([[item.pg, item.x0, item.y1, item.txt] for item in diag_els],
                          columns=['pg', 'x0', 'y1', 'txt'])

        # get indices of element type's x0 in diag_xs
        def get_idx(df, diag_xs):
            xs = []
            xs.append('time_x')
            if df['txt'].str.contains('No diagnosis').any():
                xs.append('no_diag_x')
            if df['txt'].str.contains('No diagnosis').sum() < 2:
                xs.append('dis_type_x')
            if 'dis_type_x' in xs:
                xs.append('diag_x')
            if 'diag_x' in xs and \
                    (~df[df['x0'] == diag_xs[xs.index('diag_x')]]['txt'].str.contains('Suicidality')).any():
                xs.append('symp_x')
            if df['txt'].str.contains('Suicidality').any():
                xs.append('suicid_symp_x')
            if 'suicid_symp_x' in xs:
                xs.append('desc_x')
            if 'diag_x' in xs and \
                    df[df['x0'] == diag_xs[xs.index('diag_x')]]['txt'].str.contains('Suicidality').sum() == 2:
                xs.append('desc_x')
            if 'desc_x' in xs:
                xs.append('casa_x')
            if 'diag_x' in xs and \
                    df[df['x0'] == diag_xs[xs.index('diag_x')]]['txt'].str.contains('Suicidality').sum() == 2:
                xs.append('casa_x')
            if 'casa_x' in xs:
                xs.append('comments_x')
            if 'diag_x' in xs and \
                    df[df['x0'] == diag_xs[xs.index('diag_x')]]['txt'].str.contains('Suicidality').sum() == 2:
                xs.append('comments_x')

            xs = pd.Series(
                xs
            )

            return xs

        xs = get_idx(df, diag_xs)

        # print(df.sort_values(['x0', 'y1']))

        # verify xs match length of diag_xs
        if not len(xs) == len(diag_xs):
            raise ValueError(
                f"Expected lengh of xs to match length of diagnosis x0 found {subj, event}\n" +
                f"xs: {xs}\n" +
                f"diagnosis x0s: {diag_xs}"
            )

        suicid_fields = []
        mapText = False
        for i, row in df.iterrows():
            txt = row['txt']
            # get item type
            for idx, item_type in xs.items():
                if diag_xs[int(idx)] == row['x0']:
                    break
            # print(txt, item_type)
            # get time
            if item_type == 'time_x':
                time = txt[:-len(' Diagnosis')]
            elif item_type == 'no_diag_x':
                continue
            elif item_type == 'dis_type_x':
                continue
            # items associated with columns
            elif item_type == 'diag_x' or item_type == 'symp_x':
                txt_processed = txt
                # replace present (occurs after comma or code) with current
                txt_processed = re.sub(r'[,–-] ?present', ', Current', txt_processed, flags=re.IGNORECASE)
                txt_processed = re.sub(r'[)] present', '), Current', txt_processed, flags=re.IGNORECASE)

                # replace special characters
                spec_char = {
                    b'\xef\xac\x81': 'fi',
                    "’".encode('utf-8'): "'"
                }
                for k in spec_char.keys():
                    txt_processed = txt_processed.replace(k.decode('utf-8'), spec_char[k])

                # if diagnosis
                if item_type == 'diag_x':
                    # skip if Suicidality
                    if 'Suicidality' in txt_processed:
                        continue

                    # by space if disorder (extract remission & time)
                    if re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE):
                        time = re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE).group()

                    # find field label with time
                    ind = template['Section Header'].str.contains(r'\b' + time, case=False)

                    # find remission
                    remission = re.search(
                        r"(full)|(partial) remission", txt_processed, re.IGNORECASE).group() if re.search(
                        r"(full)|(partial) remission", txt_processed, re.IGNORECASE) else ''
                    if remission:
                        ind &= template['Field Label'].str.contains(remission, case=False)
                    else:
                        ind &= ~template['Field Label'].str.contains('remission', case=False)

                    # remove time & remission from txt_processed
                    pat = r'(–?' + time + ')|(' + remission + ')'
                    txt_processed = re.sub(pat, '', txt_processed)
                    # replace newline with space
                    txt_processed = re.sub('\n', ' ', txt_processed)

                    # split by , ( or ) except in parentheses
                    tokens = []
                    splits = re.split(r',|\(|\)\s*(?![^()]*\))', txt_processed)
                    for j, el in enumerate(splits):
                        if j == 0:
                            tokens.append(el.strip())
                        else:
                            tokens.extend(el.split())

                    # special case 1: AD/H other inclusive of AD/H
                    if 'Attention-Deficit/Hyperactivity Disorder' in txt_processed:
                        if 'Other' in txt_processed:
                            ind &= template['Field Label'].str.contains('Other', case=False, regex=False)
                        else:
                            ind &= ~template['Field Label'].str.contains('Other', case=False, regex=False)
                    # special case 2: map to text instead of mapping to 1
                    if re.search('sleep problems', txt_processed, re.IGNORECASE) or re.search('insomnia', txt_processed, re.IGNORECASE):
                        mapText = True

                    # match tokens
                    for token in tokens:
                        ind &= template['Field Label'].str.contains(token, case=False, regex=False)
                        # print(template.loc[ind, 'Field Label'])
                        if ind.sum() == 1:
                            break

                # if symptom
                elif item_type == 'symp_x':
                    # by , if symptoms
                    tokens = txt_processed.split(',')
                    # extract symptom, time
                    symp = tokens[0]
                    time_matches = re.findall(r"(\bCurrent)|(\bPast)", re.sub(r' \([^)]*\)', '', txt_processed), flags=re.IGNORECASE)
                    # verify at most 1 time value
                    if len(time_matches) > 1:
                        sub = re.sub(r' \([^)]*\)', '', txt)
                        raise ValueError(
                            f"Expected at most 1 value of time from symptom\n" +
                            f"{time_matches}\n{txt_processed}\n{sub}"
                        )
                    # if 1 found, use that as time
                    elif len(time_matches) == 1:
                        time = time_matches[0][0] if time_matches[0][0] else time_matches[0][1]
                    # if 0 found use time from time_x
                    time = time.strip()
                    # find field label with symptom & time
                    # special case => stealing
                    if re.search('stealing', symp, re.IGNORECASE):
                        if 'confronting' in symp:
                            ind = template['Field Label'].str.contains(symp, case=False, regex=False)
                            ind &= template['Field Label'].str.contains('confronting', case=False, regex=False)
                        else:
                            ind = template['Field Label'].str.contains(symp, case=False, regex=False)
                            ind &= ~template['Field Label'].str.contains('confronting', case=False, regex=False)
                    else:
                        ind = template['Field Label'].str.contains(symp, case=False, regex=False)
                    ind &= template['Section Header'].str.contains(r'\b' + time)
                else:
                    pass

                # if multiple possible matches
                if ind.sum() > 1:
                    raise ValueError(
                        "Field should have at most 1 match\n" +
                        f"{txt}\n{txt_processed}\n" +
                        f"{tokens}\n" +
                        ', '.join(template.loc[ind]['Field Label'].values)
                    )
                # raise if no matches unless special case
                elif ind.sum() == 0:
                    # special case: phobia
                    if re.search('phobi', txt_processed, re.IGNORECASE):
                        continue
                    # special case: mapping text
                    elif mapText:
                        mapText = False
                        continue
                    else:
                        raise ValueError(
                            "Field should have at least 1 match\n" +
                            f"{txt}\n{txt_processed}\n" +
                            f"{tokens}"
                        )

                # add to df for mapping (value = 1)
                col = template.loc[ind]['Variable / Field Name'].values[0]
                if col in redcap_vals:
                    raise ValueError(
                        f"Field Name already exists: {col}"
                    )
                # special cases (do not map to 1 but text of next item)
                if mapText:
                    redcap_vals[col] = df.loc[i+1]['txt']
                # otherwise just map to 1
                else:
                    redcap_vals[col] = 1
            elif item_type == 'suicid_symp_x':
                # just header
                if txt == 'Symptom':
                    continue

                # replace : with -
                txt = txt.replace(':', ' -').replace('\n', ' ').replace(',', '')
                # match txt with field
                ind = template['Field Label'].str.contains(txt, case=False, regex=False)
                ind &= template['Field Label'].str.contains(time, case=False, regex=False)

                # only 1 match
                if ind.sum() != 1:
                    raise ValueError(
                        "Field should exactly 1 match\n" +
                        f"{txt}\n" +
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
                if re.search(r'C-\s?CASA\s?Code', txt):
                    continue

                # get casa code
                redcap_vals[suicid_field].append('C-CASA Code: ' + txt.replace('\n', ' '))
            elif item_type == 'comments_x':
                # just header
                if re.search(r'Patient\sComments', txt):
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

        # print(redcap_vals)
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
            # pdf.tree.write('test.xml', pretty_print=True)
            doc = pdf.pq
            # parse pdf & sort elements
            diag_els = self.sort_el_coord(self.parse_diag_elements(doc))
            info_els = self.sort_el_coord(self.parse_info_elements(doc))

            template = pd.read_csv(self._template_path)
            try:
                redcap_vals = self.parse_data(diag_els, info_els, template)
            except ValueError as err:
                errors.append(
                    KsadsUploaderError(str(err), subj_id=subj, event=event, form_path=report.report_path)
                )
                redcap_vals = None
                # raise err
            else:
                # verify subj, event matches, youth vs parent
                pdf_subj, pdf_event = redcap_vals[self.id_field()], redcap_vals[self.event_field()]
                pdf_source = 'P' if info_els[3].txt == 'Parent' else 'Y'
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
                    source = os.path.split(report.report_path)[-1][0]
                    if pdf_source != source:
                        raise KsadsUploaderError(
                            f'Form source must match: {source} vs {pdf_source, info_els[3].txt} ',
                            subj_id=subj, event=event, form_path=report.report_path
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

                # TODO: overwrite other cols to be blank?

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
