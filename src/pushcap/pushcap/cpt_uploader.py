import os
import re

import numpy as np
import pandas as pd
import xlrd

import ipdb

from . import RedcapUploader, RedcapUploaderError

class CptUploader(RedcapUploader):
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
        for (subj, event), report in self._reports.items():
            redcap_vals = {}
            # Redirect warnings to /dev/null, silences the slew of OLE2 warnings
            xlrd_xls = xlrd.open_workbook(report.report_path,
                                          logfile=open(os.devnull, 'w'))
            xls = pd.read_excel(xlrd_xls, sheet_name=None)
            for sheet_name in xls.keys():
                if sheet_name not in self._rc_map.keys():
                    continue
                for col, field in self._rc_map[sheet_name].items():
                    if ( self._skip_complete and
                         self.is_complete(subj, event, field) ):
                        continue
                    else:
                        try:
                            redcap_vals[field] = xls[sheet_name].loc[0, col].item()
                        except AttributeError:
                            redcap_vals[field] = xls[sheet_name].loc[0, col]
                        completed_field = self.completed_field(field)
                        redcap_vals[completed_field] = self._uploaded_status

            if redcap_vals:
                redcap_vals[self.id_field()] = subj
                redcap_vals[self.event_field()] = event
                pulled_data.append(redcap_vals)

        return pulled_data, errors

    def parse_template(self, template_path):
        """
        Assumptions: header row with a single data row underneath. Values of
        interest are replaced with REDCap field names, everything else is blank.
        """
        rc_map = {}
        # Redirect warnings to /dev/null, silences the slew of OLE2 warnings
        xlrd_xls = xlrd.open_workbook(
                template_path, logfile=open(os.devnull, 'w'))
        xls = pd.read_excel(xlrd_xls, sheet_name=None)
        for sheet_name in xls.keys():
            if xls[sheet_name].shape[0] != 1:
                raise CptUploaderError(
                        'Template file has >1 data rows, expecting 1.')
            data_row = xls[sheet_name].loc[0]
            rc_map[sheet_name] = {col: val for col, val in data_row.iteritems()
                                  if not pd.isna(val)}

        return rc_map

    def api_url(self):
        return self._api_url

    def token(self):
        return self._token

    def log_path(self):
        return self._log_path

    def date_fields(self):
        return self._date_fields


class CptReport:
    def __init__(self, report_path):
        self.report_path = report_path


class CptUploaderError(RedcapUploaderError):
    pass
