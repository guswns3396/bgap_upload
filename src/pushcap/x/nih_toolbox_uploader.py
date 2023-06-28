import csv
from datetime import datetime
from dateutil.parser import parse as dateparse
from pathlib import Path
import re

from . import RedcapUploader, RedcapUploaderError

class NIHToolboxUploader(RedcapUploader):
    def __init__(self, template_path, csv_tuples, api_url, token, log_path,
                 overwrite_ok=None, uploaded_status=None,
                 skip_complete=True):
        """Initializes an NIHToolboxUploader instance with data & API info.

        Arguments:
            template_path: Path to template file per the class description.
            csv_tuples: Tuples of the form (subj_id, event, csv_path) where
              subj_id is a valid record ID in REDCap, timepoint is a valid event
              name in REDCap, and csv_path points to their exported NIH Toolbox
              scores csv.
            api_url: API URL for the REDCap database.
            token: API token for the REDCap database.
            log_path: Path where we will log the JSON string that we send to
              REDCap for the push. For multiple pushes, this should be changed
              via self.change_log_path to avoid writing over previous log files.
        """
        self._api_url = api_url
        self._token = token
        super().__init__()

        self._csv_tuples = []
        for subj_id, event, csv_path in csv_tuples:
            for prev_subj_id, prev_event, csv_paths in self._csv_tuples:
                if subj_id == prev_subj_id and event == prev_event:
                    csv_paths.append(csv_path)
                    break
            else:
                self._csv_tuples.append((subj_id, event, [csv_path]))
            
        self._log_path = log_path
        self._overwrite_ok = [self.id_field(), self.event_field()]
        self._skip_complete = skip_complete
        if overwrite_ok is not None:
            self._overwrite_ok += overwrite_ok
        if not uploaded_status:
            self._uploaded_status = self.UNVERIFIED
        else:
            self._uploaded_status = uploaded_status

        self._rcmap = NIHToolboxUploader.parse_template(template_path)
        bad_redcap_fields = [v[1] for inst in self._rcmap.values() for v in inst
                             if v[1] not in self.field_names()]
        if bad_redcap_fields:
            raise NIHTbUploaderError(
                    'These field(s) do not exist in the REDCap database:\n' +
                    ", ".join(bad_redcap_fields))

    def pull(self):
        errors = []
        pulled_data = []
        for p in self._csv_tuples:
            try:
                data = self.parse_csv(p[0], p[1], p[2][0])
                for extra_csv in p[2][1:]:
                    extra_data = self.parse_csv(p[0], p[1], extra_csv)
                    dupes = [k for k in extra_data.keys() if k in data.keys()
                             and k not in self._overwrite_ok]
                    if dupes:
                        raise NIHTbUploaderError('Multiple values for ' +
                                                 ', '.join(dupes) + '.')
                    data.update(extra_data) 
            except RedcapUploaderError as err:
                errors.append((p[0], p[1], p[2], str(err)))
                continue

            if data:
                pulled_data.append(data)

        if errors:
            print('Skipped the following records due to errors:')
        for subj_id, event, csv_paths, err in errors:
            print(f'\n{subj_id}, {event}: {err}')
            for p in csv_paths:
                print(f'  {p}')

        return pulled_data

    def api_url(self):
        return self._api_url

    def token(self):
        return self._token

    def log_path(self):
        return self._log_path

    def change_log_path(self, new_log_path):
        self._log_path = new_log_path

    def parse_csv(self, subj_id, event, csv_path):
        print(f'Parsing {subj_id}, {event}...')
        redcap_vals = {}
        overwrites = []

        with open(csv_path) as csv_file:
            for r in csv.DictReader(csv_file):
                try:
                    instr = r['Inst']
                except KeyError:
                    raise NIHTbUploaderError('No Inst column.')

                # Skip summary, instructions, "intros", and extra header rows
                if ('Summary' in instr or 'Instructions' in instr or
                    'Intro' in instr or instr == 'Inst'):
                    continue

                if subj_id not in r['PIN']:
                    print(f'PIN mismatch (ID: {subj_id}, PIN: {r["PIN"]})')
                if instr not in self._rcmap.keys():
                    raise NIHTbUploaderError(f'Unknown instrument {instr}.')

                for csv_field, redcap_field, translate in self._rcmap[instr]:
                    if (redcap_field in redcap_vals.keys()
                            and redcap_field not in self._overwrite_ok):
                        overwrites += [redcap_field]
                        continue

                    if ( self._skip_complete and
                         self.is_complete(subj_id, event, redcap_field) ):
                        continue
                    else:
                        completed_field = self.completed_field(redcap_field)
                        redcap_vals[completed_field] = self._uploaded_status

                    # Check if it's an instrument 'global' (a value we set just
                    # based on an instrument's existence):
                    if csv_field.startswith('exists_'):
                        redcap_vals[redcap_field] = translate()
                        continue

                    try:
                        redcap_vals[redcap_field] = translate(r[csv_field])
                    except KeyError:
                        raise NIHTbUploaderError(
                            f'Missing column {csv_field} for {instr}.')
            
        if overwrites:
            raise NIHTbUploaderError(
                    f'Multiple values for {", ".join(set(overwrites))}.')
        if redcap_vals:
            redcap_vals[self.id_field()] = subj_id
            redcap_vals[self.event_field()] = event

        return redcap_vals

    def parse_template(path):
        """Parses an NIHToolboxUploader template file.

        Parses an NIHToolboxUploader template file creating a translation
        between instrument scores in NIH Toolbox and fields in REDCap. See the
        class docstring / README for more information on the template file, and
        see the make_redcap_map function for details on the translation tuples.

        Arguments:
          path: Path to the template file.

        Returns:
          Dictionary mapping instrument names to translation tuples.

        Raises NIHTbUploaderError if there is no "Inst" column in the template
        file or if template file entries don't match expected specifications
        from the README.
        """
        with open(path) as template_file:
            try:
                redcap_map = {r['Inst']: NIHToolboxUploader.make_redcap_map(r)
                              for r in csv.DictReader(template_file)}
            except KeyError:
                raise NIHTbUploaderError('No Inst column in template file.')

        return redcap_map

    def make_redcap_map(csv_row_dict):
        """ Makes tuples specifying NIHTb output > REDCap input translations.

        Takes a line from a NIHToolboxUploader template csv and creates a tuple
        that describes the mapping between that field in an NIHToolbox output
        file and a field in REDCap. Expects a particular syntax in the template
        file: it should be a csv file matching the columns and instruments from
        a normal output file, but instead of output values in each row, there
        is a REDCap field identifier and optional instructions for translation.
        See the README.md of this repo for more details on the template files.

        Arguments:
          csv_row_dict: An OrderedDict, one row returned from csv.DictReader.

        Returns:
          A list of tuples of the form (csv_field, redcap_field, translate_fn):
            csv_field: csv column name in the NIH Toolbox output files.
            redcap_field: The field name in REDCap that corresponds to
              csv_field for this instrument row.
            translate_fn: a lambda function that converts the value for this
              instrument row and csv_field into the desired value for REDCap,
              per the specifications in the template file (see the README).

        Raises NIHTbUploaderError if any non-empty row value does not match
        the template file specifications.
        """

        redcap_re = re.compile(r'^\{([A-Za-z0-9_]+)\}'
                               r'(?:\((datetime|constant) (.+)\))?$')
        strip_0s = lambda x: re.sub(r'^(-?\d+)(?:(?:(\.\d*[1-9])0+)|(?:\.0+))$',
                                     r'\1\2', x)

        xlate_tuples = []
        for csv_field, val in csv_row_dict.items():
            if (csv_field == 'Inst') or not val: continue

            match = redcap_re.match(val)
            if val and not match:
                raise NIHTbUploaderError(f'Invalid template spec: {val}')

            # REDCap field
            redcap_field = match.group(1)

            if (csv_field.startswith('exists_') and not
                    (match.group(2) and match.group(2) == 'constant')):
                raise NIHTbUploaderError(
                        f'Template error for column {csv_field}, value {val}:'
                         'any "exists" column must have a value of the form '
                         '{<redcap_field>}(constant <value>), e.g., '
                         '{demographics_age}(constant 9).')

            # Translation function
            if not match.group(2):
                fn = strip_0s
            elif match.group(2) == 'datetime':
                fmt = match.group(3)
                fn = lambda x : dateparse(x).strftime(fmt)
            elif match.group(2) == 'constant':
                constant = match.group(3)
                fn = lambda : constant

            xlate_tuples.append((csv_field, match.group(1), fn))

        return xlate_tuples

class NIHTbUploaderError(RedcapUploaderError):
    """Exception class for parsing errors in NIHTb output or template files."""
    pass
