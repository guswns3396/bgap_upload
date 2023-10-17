import re
import os

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
        subj, event, date_field = None, None, None
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

        if self.id_field() not in redcap_vals:
            raise ValueError(
                f'Could not parse ID\n' +
                f'{[i.txt for i in info_els]}'
            )
        if self.event_field() not in redcap_vals:
            raise ValueError(
                f'Could not parse event\n' +
                f'{[i.txt for i in info_els]}'
            )
        if date_field is None:
            raise ValueError(
                f'Could not parse date\n' +
                f'{[i.txt for i in info_els]}'
            )

        # parse diagnosis data
        df = pd.DataFrame([[item.pg, item.x0, item.y1, item.txt] for item in diag_els],
                          columns=['pg', 'x0', 'y1', 'txt'])

        # get indices of element type's x0 in diag_xs
        def get_idx(df_arg, diag_xs_arg):
            xs_arr = ['time_x']
            if df_arg['txt'].str.contains('No diagnosis').any():
                xs_arr.append('no_diag_x')
            if df_arg['txt'].str.contains('No diagnosis').sum() < 2:
                xs_arr.append('dis_type_x')
            if 'dis_type_x' in xs_arr:
                xs_arr.append('diag_x')
            if 'diag_x' in xs_arr and \
                    (
                        ~df_arg[df_arg['x0'] == diag_xs_arg[xs_arr.index('diag_x')]]['txt'].str.contains('Suicidality')
                    ).any():
                xs_arr.append('symp_x')
            if df_arg['txt'].str.contains('Suicidality').any():
                xs_arr.append('suicid_symp_x')
            if 'suicid_symp_x' in xs_arr:
                xs_arr.append('desc_x')
            if 'diag_x' in xs_arr and \
                    df_arg[df_arg['x0'] == diag_xs_arg[xs_arr.index('diag_x')]]['txt'].str.contains(
                        'Suicidality'
                    ).sum() == 2:
                xs_arr.append('desc_x')
            if 'desc_x' in xs_arr:
                xs_arr.append('casa_x')
            if 'diag_x' in xs_arr and \
                    df_arg[df_arg['x0'] == diag_xs_arg[xs_arr.index('diag_x')]]['txt'].str.contains(
                        'Suicidality'
                    ).sum() == 2:
                xs_arr.append('casa_x')
            if 'casa_x' in xs_arr:
                xs_arr.append('comments_x')
            if 'diag_x' in xs_arr and \
                    df_arg[df_arg['x0'] == diag_xs_arg[xs_arr.index('diag_x')]]['txt'].str.contains(
                        'Suicidality'
                    ).sum() == 2:
                xs_arr.append('comments_x')

            xs_arr = pd.Series(
                xs_arr
            )

            return xs_arr

        xs = get_idx(df, diag_xs)

        # print(df.sort_values(['x0', 'y1']))
        # print(df)

        # verify xs match length of diag_xs
        if not len(xs) == len(diag_xs):
            raise ValueError(
                f"Expected lengh of xs to match length of diagnosis x0 found {subj, event}\n" +
                f"xs: {xs}\n" +
                f"diagnosis x0s: {diag_xs}"
            )

        def parse_time_x(txt_arg):
            time_str = txt_arg[:-len(' Diagnosis')]
            return time_str

        def process_txt(txt_arg):
            txt_processed = txt_arg
            # replace present (occurs after comma or code) with current
            txt_processed = re.sub(r'[,–-] ?present', ', Current', txt_processed, flags=re.IGNORECASE)
            txt_processed = re.sub(r'[)] present', '), Current', txt_processed, flags=re.IGNORECASE)
            # replace newline with space
            txt_processed = re.sub('\n', ' ', txt_processed)
            # replace special characters
            spec_char = {
                b'\xef\xac\x81': 'fi',
                "’".encode('utf-8'): "'"
            }
            for k in spec_char.keys():
                txt_processed = txt_processed.replace(k.decode('utf-8'), spec_char[k])
            return txt_processed

        def verify_match(ind_arg, template_arg, tokens_arg, txt_arg, txt_processed_arg):
            # if multiple possible matches
            if ind_arg.sum() > 1:
                raise ValueError(
                    "Field should have at most 1 match\n" +
                    f"{txt_arg}\n{txt_processed_arg}\n" +
                    f"{tokens_arg}\n" +
                    ', '.join(template_arg.loc[ind_arg]['Field Label'].values)
                )
            # raise if no matches unless special case
            elif ind_arg.sum() == 0:
                raise ValueError(
                    "Field should have at least 1 match\n" +
                    f"{txt_arg}\n{txt_processed_arg}\n" +
                    f"{tokens_arg}"
                )

        def map_col(ind_arg, template_arg, redcap_vals_arg, maptext_arg, txt_arg):
            # add to df for mapping
            col = template_arg.loc[ind_arg]['Variable / Field Name'].values[0]
            if col in redcap_vals_arg:
                # special case 1: duplicated symptom
                if redcap_vals_arg[col] == txt_arg or redcap_vals_arg[col] == 1:
                    pass
                else:
                    raise ValueError(
                        f"Field Name already exists: {col}\n{redcap_vals_arg[col]}"
                    )
            # map to text
            if maptext_arg:
                redcap_vals_arg[col] = txt_arg
            # map to 1
            else:
                redcap_vals_arg[col] = 1
            return redcap_vals_arg

        def parse_diag_x(txt_arg, curr_vars_arg, redcap_vals_arg, template_arg):
            # preprocess txt
            txt_processed = process_txt(txt_arg)

            # skip if Suicidality
            if 'Suicidality' in txt_processed:
                return redcap_vals_arg, curr_vars_arg

            # store txt as diagnosis
            curr_vars_arg['diag'] = txt_processed

            # find time & match
            if re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE):
                time_str = re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE).group()
            else:
                time_str = curr_vars_arg['time']
            ind_arr = template_arg['Section Header'].str.contains(r'\b' + time_str, case=False)

            # find remission & match
            remission = re.search(
                r"(full)|(partial) remission", txt_processed, re.IGNORECASE).group() if re.search(
                r"(full)|(partial) remission", txt_processed, re.IGNORECASE) else ''
            if remission:
                ind_arr &= template_arg['Field Label'].str.contains(remission, case=False)
            else:
                ind_arr &= ~template_arg['Field Label'].str.contains('remission', case=False)

            # remove time & remission from txt_processed
            pat = r'(–?' + time_str + ')|(' + remission + ')'
            txt_processed = re.sub(pat, '', txt_processed)

            # split by , ( or ) except in parentheses
            # get tokens
            tokens_arr = []
            splits = re.split(r',|\(|\)\s*(?![^()]*\))', txt_processed)
            for j, el in enumerate(splits):
                if j == 0:
                    tokens_arr.append(el.strip())
                else:
                    tokens_arr.extend(el.split())

            # special case 1: AD/H other vs AD/H
            if 'Attention-Deficit/Hyperactivity Disorder' in txt_processed:
                if 'Other' in txt_processed:
                    ind_arr &= template_arg['Field Label'].str.contains('Other', case=False, regex=False)
                else:
                    ind_arr &= ~template_arg['Field Label'].str.contains('Other', case=False, regex=False)
            # special case 2: if sleep problems => just continue;
            # map based on symptom "patient reported trouble falling asleep"
            # 'insomnia' variable not used?
            if re.search('sleep problems', txt_processed, re.IGNORECASE) or \
                    re.search('insomnia', txt_processed, re.IGNORECASE):
                return redcap_vals_arg, curr_vars_arg
            # special case 3: phobia not part of KSADS
            if re.search('phobi', txt_processed, re.IGNORECASE):
                return redcap_vals_arg, curr_vars_arg
            # special case 4: adjustment disorder not part of KSADS
            if re.search('adjustment disorder', txt_processed, re.IGNORECASE):
                return redcap_vals_arg, curr_vars_arg
            # special case 7: disruptive mood dysregulation has no time
            if re.search('disruptive mood dysregulation', txt_processed, re.IGNORECASE):
                ind_arr |= ~template_arg['Section Header'].str.contains(r'\b' + time_str, case=False)
            # special case 8: bipolar I disorder has no remission
            if re.search('bipolar I disorder', txt_processed, re.IGNORECASE):
                ind_arr |= ~template_arg['Field Label'].str.contains(remission, case=False)


            # match tokens
            for token in tokens_arr:
                ind_arr &= template_arg['Field Label'].str.contains(token, case=False, regex=False)
                if ind_arr.sum() == 1:
                    break

            # verify match
            verify_match(ind_arg=ind_arr, template_arg=template_arg, tokens_arg=tokens_arr, txt_arg=txt_arg,
                         txt_processed_arg=txt_processed)

            # map
            redcap_vals_arg = map_col(ind_arr, template_arg, redcap_vals_arg, maptext_arg=False, txt_arg=txt_processed)
            return redcap_vals_arg, curr_vars_arg

        def parse_symp_x(txt_arg, curr_vars_arg, redcap_vals_arg, template_arg):
            mapText = False
            # preprocess txt
            txt_processed = process_txt(txt_arg)

            # split by , if symptoms
            tokens = txt_processed.split(',')

            # extract symptom, time
            symp = tokens[0]
            time_matches = re.findall(r"(\bCurrent)|(\bPast)", re.sub(r' \([^)]*\)', '', txt_processed),
                                      flags=re.IGNORECASE)

            # verify at most 1 time value
            if len(time_matches) > 1:
                sub = re.sub(r' \([^)]*\)', '', txt_arg)
                raise ValueError(
                    f"Expected at most 1 value of time from symptom\n" +
                    f"{time_matches}\n{txt_processed}\n{sub}"
                )
            # if 1 found, use that as time
            elif len(time_matches) == 1:
                time_str = time_matches[0][0] if time_matches[0][0] else time_matches[0][1]
            # if 0 found use time from time_x
            else:
                time_str = curr_vars_arg['time'].strip()

            # find field label with symptom & time
            ind_arr = template_arg['Field Label'].str.contains(symp, case=False, regex=False)
            ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time_str)

            # special case 1: stealing
            if re.search('stealing', symp, re.IGNORECASE):
                if 'confronting' in symp:
                    ind_arr &= template_arg['Field Label'].str.contains('confronting', case=False, regex=False)
                else:
                    ind_arr &= ~template_arg['Field Label'].str.contains('confronting', case=False, regex=False)
            # special case 2: irritability vs explosive irritability vs manic irritability
            if re.search('irritability', symp, re.IGNORECASE):
                if 'Explosive' in txt_processed:
                    ind_arr &= template_arg['Field Label'].str.contains('Explosive', case=False, regex=False)
                elif 'Manic' in txt_processed:
                    ind_arr &= template_arg['Field Label'].str.contains('Manic', case=False, regex=False)
                else:
                    ind_arr &= ~template_arg['Field Label'].str.contains('Explosive', case=False, regex=False)
                    ind_arr &= ~template_arg['Field Label'].str.contains('Manic', case=False, regex=False)
            # special case 3: suicidal ideation as symptom
            if re.match('^suicidal ideation$', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.fullmatch(
                    '^suicidal ideation: ' + time_str + '$',
                    case=False
                )
            # special case 4: sleep problem => map to text
            if re.search('Patient reported trouble falling asleep or staying asleep', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains('sleep problems', case=False, regex=False)
                # use time from time_x since contains "past"
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + curr_vars_arg['time'])
                mapText = True
            # special case 5: phobia not part of KSADS
            if re.search('phobi', symp, re.IGNORECASE):
                return redcap_vals_arg
            # special case 6: adjustment disorder not part of KSADS
            # adjustment disorder symptom if diag == adjustment disorder and no symptoms matched
            if ind_arr.sum() == 0 and re.search('adjustment disorder', curr_vars_arg['diag'], re.IGNORECASE):
                return redcap_vals_arg
            # special case 7: disruptive mood dysregulation symptoms don't have time
            if ind_arr.sum() == 0 and \
                    re.search('disruptive mood dysregulation', curr_vars_arg['diag'], re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(symp, case=False, regex=False)
            # special case 8: 'Difficulty sustaining attention since elementary school'
            # vs 'more than one school year'
            if re.search('Difficulty sustaining', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(
                    'Difficulty sustaining',
                    case=False, regex=False
                )
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time_str)
            # special case 9: 'Easily distracted since elementary school'
            # vs 'for more than one school year'
            if re.search('easily distracted', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(
                    'easily distracted',
                    case=False, regex=False
                )
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time_str)
            # special case 10: 'Difficulty remaining seated since elementary school'
            # vs 'for more than one school year'
            if re.search('Difficulty remaining seated', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(
                    'Difficulty remaining seated',
                    case=False, regex=False
                )
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time_str)
            # special case 11: elevated / euphoric mood => elevated mood
            if re.search('Elevated', symp, re.IGNORECASE) and re.search('mood', symp, re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(
                    'Elevated mood:',
                    case=False, regex=False
                )
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time_str)
            # special case 12: hypersexuality
            if re.search('Hypersexuality', symp, re.IGNORECASE):
                return redcap_vals_arg
            # special case 13: distractibility vs increased distractibility
            if re.search('distractibility', symp, re.IGNORECASE):
                if 'Increased' in symp:
                    ind_arr &= template_arg['Field Label'].str.contains('increased', case=False, regex=False)
                else:
                    ind_arr &= ~template_arg['Field Label'].str.contains('increased', case=False, regex=False)

            # verify match
            verify_match(ind_arg=ind_arr, template_arg=template_arg, tokens_arg=tokens, txt_arg=txt_arg,
                         txt_processed_arg=txt_processed)

            redcap_vals_arg = map_col(ind_arr, template_arg, redcap_vals_arg, maptext_arg=mapText, txt_arg=txt_arg)
            return redcap_vals_arg

        def parse_suicide_symp_x(txt_arg, curr_vars_arg, redcap_vals_arg, template_arg, suicide_fields_arg):
            # just header
            if txt_arg == 'Symptom':
                return curr_vars_arg, redcap_vals_arg, suicide_fields_arg

            # replace : with -
            txt_processed = txt_arg.replace(':', ' -').replace('\n', ' ').replace(',', '')
            txt_processed = txt_processed.replace('Self- ', 'Self-')

            # get time
            if re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE):
                time = re.search(r"(\bCurrent)|(\bPast)", txt_processed, re.IGNORECASE).group()
            else:
                time = curr_vars_arg['time']

            # match txt with field
            ind_arr = template_arg['Field Label'].str.contains(txt_processed, case=False, regex=False)
            ind_arr &= template_arg['Field Label'].str.contains(time, case=False, regex=False)

            # special case 1: "suicide attempt" matches multiple
            if re.match('^suicide attempt$', txt_processed.strip(), re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.fullmatch(
                    '^' + time + ' suicide attempt$',
                    case=False
                )
            # special case 2: preparatory actions toward imminent suicidal behavior
            prep_act_str = 'preparatory actions toward imminent suicidal behavior'
            if re.search(prep_act_str, txt_processed.strip(), re.IGNORECASE):
                ind_arr = template_arg['Field Label'].str.contains(prep_act_str, case=False, regex=False)
                ind_arr &= template_arg['Section Header'].str.contains(r'\b' + time, case=False)

            # verify match
            verify_match(ind_arr, template_arg, tokens_arg=None, txt_arg=txt_arg, txt_processed_arg=txt_processed)

            # store suicide field for completion at next iterations
            suicide_field = template_arg.loc[ind_arr]['Variable / Field Name'].values[0]
            curr_vars_arg['suicide_field'] = suicide_field
            redcap_vals_arg[suicide_field] = []
            suicide_fields_arg.append(suicide_field)

            return curr_vars_arg, redcap_vals_arg, suicide_fields_arg

        def parse_suicide_desc_x(txt_arg, curr_vars_arg, redcap_vals_arg):
            # just header
            if txt_arg == 'Description':
                return redcap_vals_arg
            # get description
            suicide_field = curr_vars_arg['suicide_field']
            redcap_vals_arg[suicide_field].append("Description: " + txt_arg.replace('\n', ' '))
            return redcap_vals_arg

        def parse_suicide_casa_x(txt_arg, curr_vars_arg, redcap_vals_arg):
            # just header
            if re.search(r'C-\s?CASA\s?Code', txt_arg):
                return redcap_vals_arg

            # get casa code
            suicide_field = curr_vars_arg['suicide_field']
            redcap_vals_arg[suicide_field].append('C-CASA Code: ' + txt_arg.replace('\n', ' '))
            return redcap_vals_arg

        def parse_suicide_comments_x(txt_arg, curr_vars_arg, redcap_vals_arg):
            # just header
            if re.search(r'Patient\sComments', txt_arg):
                return redcap_vals_arg

            # get patient comments
            suicide_field = curr_vars_arg['suicide_field']
            redcap_vals_arg[suicide_field].append('Patient Comments: ' + txt_arg.replace('\n', ' '))
            return redcap_vals_arg

        suicide_fields = []
        curr_vars = {}
        for i, row in df.iterrows():
            txt = row['txt']
            # get item type
            for idx, item_type in xs.items():
                if diag_xs[int(idx)] == row['x0']:
                    break
            # print(txt, item_type)
            # get time
            if item_type == 'time_x':
                curr_vars['time'] = parse_time_x(txt)
            elif item_type == 'no_diag_x':
                continue
            elif item_type == 'dis_type_x':
                continue
            elif item_type == 'diag_x':
                redcap_vals, curr_vars = parse_diag_x(txt, curr_vars, redcap_vals, template)
            elif item_type == 'symp_x':
                redcap_vals = parse_symp_x(txt, curr_vars, redcap_vals, template)
            elif item_type == 'suicid_symp_x':
                curr_vars, redcap_vals, suicide_fields = parse_suicide_symp_x(txt, curr_vars, redcap_vals, template, suicide_fields)
            elif item_type == 'desc_x':
                redcap_vals = parse_suicide_desc_x(txt, curr_vars, redcap_vals)
            elif item_type == 'casa_x':
                redcap_vals = parse_suicide_casa_x(txt, curr_vars, redcap_vals)
            elif item_type == 'comments_x':
                redcap_vals = parse_suicide_comments_x(txt, curr_vars, redcap_vals)
            else:
                raise ValueError(
                    f"Invalid item_type: {item_type}"
                )
        # turn suicidality fields from list to single string
        for suicide_field in suicide_fields:
            redcap_vals[suicide_field] = ' | '.join(redcap_vals[suicide_field])

        # print(redcap_vals)
        return redcap_vals

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
            try:
                diag_els = self.sort_el_coord(self.parse_diag_elements(doc))
                info_els = self.sort_el_coord(self.parse_info_elements(doc))
            except TypeError as err:
                errors.append(
                    KsadsUploaderError(str(err), subj_id=subj, event=event, form_path=report.report_path)
                )
                continue

            template = pd.read_csv(self._template_path)
            try:
                redcap_vals = self.parse_data(diag_els, info_els, template)
            except ValueError as err:
                errors.append(
                    KsadsUploaderError(str(err), subj_id=subj, event=event, form_path=report.report_path)
                )
                redcap_vals = None
                # raise err
            except KeyError as err:
                errors.append(
                    KsadsUploaderError(str(err), subj_id=subj, event=event, form_path=report.report_path)
                )
                redcap_vals = None
            else:
                # verify subj, event matches, youth vs parent
                pdf_subj, pdf_event = redcap_vals[self.id_field()], redcap_vals[self.event_field()]

                # function to get source element
                def get_source_element(element):
                    if element.txt in ['Parent', 'Youth', 'Teen']:
                        return True
                    return False

                try:
                    # get source
                    filtered = list(filter(get_source_element, info_els))
                    if len(filtered) != 1:
                        raise KsadsUploaderError(
                            f'Error parsing source information: {[i.txt for i in filtered]}',
                            subj_id=subj, event=event, form_path=report.report_path
                        )
                    else:
                        pdf_source = 'P' if filtered[0].txt == 'Parent' else 'Y'
                    # verify subj
                    if pdf_subj != subj:
                        raise KsadsUploaderError(
                            f'Form subject ID {pdf_subj} does not '
                            f'match the provided subject ID.', subj_id=subj,
                            event=event, form_path=report.report_path
                        )
                    # verify event
                    if pdf_event != event:
                        raise KsadsUploaderError(
                            f'Form timepoint {pdf_event} does not '
                            f'match the provided timepoint.', subj_id=subj,
                            event=event, form_path=report.report_path
                        )
                    source = os.path.split(report.report_path)[-1][0]
                    # verify source
                    if pdf_source != source:
                        raise KsadsUploaderError(
                            f'Form source must match: {source} vs {pdf_source, info_els[3].txt} ',
                            subj_id=subj, event=event, form_path=report.report_path
                        )
                except KsadsUploaderError as err:
                    errors.append(err)
                    continue

                # skip if already complete
                try:
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
                except RedcapUploaderError as err:
                    errors.append(err)
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
