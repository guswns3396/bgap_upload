#!/usr/bin/env python3
from datetime import datetime
from pathlib import Path
import re
import time
import ipdb

from pushcap import (RedcapUploader, RedcapUploaderError,
                     NIHToolboxUploader, NIHTbReport,
                     QInteractiveUploader, QInteractiveUploaderError,
                     QInteractiveReport,
                     DkefsUploader, DkefsReport,
                     QGlobalUploader, QGlobalReport,
                     CptUploader, CptReport)


def bgap_crawl(data_path, report_handlers):
    report_matches = {report_type: {} for report_type in report_handlers.keys()}
    errors = []

    for subject_event_dir in data_path.glob('*/Year [1234]/DataExports'):
        subj_id = subject_event_dir.parts[-3]
        tp = subject_event_dir.parts[-2][-1]
        event = f'year_{tp}_arm_1'
        print(f'\nChecking {subj_id}, {event}...')

        for report_path in subject_event_dir.iterdir():
            for report_type, (report_regex, _, _) in report_handlers.items():

                m = report_regex.fullmatch(str(report_path.resolve()))
                if not m: continue

                if subj_id != m.group('id'):
                    errors.append(RedcapUploaderError(
                            f'Subj ID from directory ({subj_id}) does not '
                            f'match file name ID ({m.group("id")}).',
                            subj_id=subj_id, event=event,
                            form_id=report_type,
                            form_path=report_path))
                    break

                elif tp != m.group('tp'):
                    errors.append(RedcapUploaderError(
                            f'Timepoint from directory ({tp}) does not '
                            f'match file name timepoint ({m.group("tp")}).',
                            subj_id=subj_id, event=event,
                            form_id=report_type,
                            form_path=report_path))
                    break

                relative_path = Path(m.group(0)).relative_to(data_path)
                print(f'  {report_type:11} : {relative_path}')
                try:
                    report_matches[report_type][(subj_id, event)].append(m)
                except KeyError:
                    report_matches[report_type][(subj_id, event)] = [m, ]
                break

    return report_matches, errors


def make_nihtb_uploader(matches, template_path, api_url, token, log_path):
    reports = {(sbj, evt): NIHTbReport([Path(m.group(0)) for m in report_ms])
               for (sbj, evt), report_ms in matches.items()}
    if not reports:
        return None, []
    else:
        return NIHToolboxUploader(reports, template_path, api_url, token,
                                  log_path, overwrite_ok=('nih_remote', )), []


def make_wisc_uploader0(matches, template_path, api_url, token, log_path):
    errors = []
    reports = {}

    # Function to convert date from KTEA info file to a nice REDCap format
    date_fn = lambda d: datetime.strptime(d, '%B %d %Y').strftime('%Y-%m-%d')
    date_mapping = {'Scheduled Assessment Date': ('wisc_date', date_fn)}

    for (subj_id, event), report_matches in matches.items():
        # More than 1 WISC form? Error
        if len(report_matches) != 1:
            errors += [QInteractiveUploaderError(
                            f'Expecting 1 report, got {len(report_matches)}.',
                            subj_id=subj_id, event=event, form_id='wisc',
                            form_path=report_match.group(0))
                       for report_match in report_matches]
            continue
        
        scores_path = Path(report_matches[0].group(0))

        # Find info file
        tp = event.split('_')[1]
        info_file_re = re.compile(
                f'.*/{subj_id}_{tp}' +
                r'_(?P<month>\d\d?)_(?P<day>\d\d?)_(?P<year>\d{4})' +
                r'(?:NEW )?(?:Remote *)?WISC(?:-V)?' +
                r'(?: *\(Core 10\))?(?: New)?_information.txt')
        info_matches = [m for m in [info_file_re.fullmatch(str(f.resolve()))
                                    for f in scores_path.parent.iterdir()] if m]

        # More/less than 1 WISC info file? Error
        if len(info_matches) != 1:
            errors.append(QInteractiveUploaderError(
                    f'Expecting 1 WISC info file, got {len(info_matches)}.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        info_path = Path(info_matches[0].group(0))
        subj_report = QInteractiveReport(scores_path, info_path)

        # Check the info file scheduled assessment date against the file name
        path_date = datetime(int(info_matches[0].group('year')),
                             int(info_matches[0].group('month')),
                             int(info_matches[0].group('day'))
                            ).strftime('%Y-%m-%d')
        info_date = subj_report.extract_info(date_mapping)['wisc_date']
        if info_date != path_date:
            errors.append(QInteractiveUploaderError(
                    f'Assessment date in info file does not match path date.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        reports[(subj_id, event)] = subj_report

    if not reports:
        wisc_upl = None
    else:
        wisc_upl = QInteractiveUploader(reports, template_path, api_url, token,
                                        log_path, info_mappings=date_mapping)
    return wisc_upl, errors



def make_wisc_uploader1(matches, template_path, api_url, token, log_path):
    errors = []
    reports = {}

    # Function to convert date from KTEA info file to a nice REDCap format
    date_fn = lambda d: datetime.strptime(d, '%B %d %Y').strftime('%Y-%m-%d')
    date_mapping = {'Scheduled Assessment Date': ('wisc_date', date_fn)}

    for (subj_id, event), report_matches in matches.items():
        # More than 1 WISC form? Error
        if len(report_matches) != 1:
            errors += [QInteractiveUploaderError(
                            f'Expecting 1 report, got {len(report_matches)}.',
                            subj_id=subj_id, event=event, form_id='wisc',
                            form_path=report_match.group(0))
                       for report_match in report_matches]
            continue
        
        scores_path = Path(report_matches[0].group(0))

        # Find info file
        tp = event.split('_')[1]
        info_file_re = re.compile(
                f'.*/{subj_id}_{tp}' +
                r'_(?P<month>\d\d?)_(?P<day>\d\d?)_(?P<year>\d{4})' +
                r'(?:NEW )?(?:Remote *)?WISC(?:-V)?' +
                r'(?: *\(Core 10\))?(?: Part ?1)(?: New)?_information.txt')
        info_matches = [m for m in [info_file_re.fullmatch(str(f.resolve()))
                                    for f in scores_path.parent.iterdir()] if m]

        # More/less than 1 WISC info file? Error
        if len(info_matches) != 1:
            errors.append(QInteractiveUploaderError(
                    f'Expecting 1 WISC info file, got {len(info_matches)}.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        info_path = Path(info_matches[0].group(0))
        subj_report = QInteractiveReport(scores_path, info_path)

        # Check the info file scheduled assessment date against the file name
        path_date = datetime(int(info_matches[0].group('year')),
                             int(info_matches[0].group('month')),
                             int(info_matches[0].group('day'))
                            ).strftime('%Y-%m-%d')
        info_date = subj_report.extract_info(date_mapping)['wisc_date']
        if info_date != path_date:
            errors.append(QInteractiveUploaderError(
                    f'Assessment date in info file does not match path date.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        reports[(subj_id, event)] = subj_report

    if not reports:
        wisc_upl = None
    else:
        wisc_upl = QInteractiveUploader(reports, template_path, api_url, token,
                                        log_path, info_mappings=date_mapping)
    return wisc_upl, errors

def make_wisc_uploader2(matches, template_path, api_url, token, log_path):
    errors = []
    reports = {}

    # Function to convert date from KTEA info file to a nice REDCap format
    date_fn = lambda d: datetime.strptime(d, '%B %d %Y').strftime('%Y-%m-%d')
    date_mapping = {'Scheduled Assessment Date': ('wisc_date', date_fn)}

    for (subj_id, event), report_matches in matches.items():
        # More than 1 WISC form? Error
        if len(report_matches) != 1:
            errors += [QInteractiveUploaderError(
                            f'Expecting 1 report, got {len(report_matches)}.',
                            subj_id=subj_id, event=event, form_id='wisc',
                            form_path=report_match.group(0))
                       for report_match in report_matches]
            continue
        
        scores_path = Path(report_matches[0].group(0))

        # Find info file
        tp = event.split('_')[1]
        info_file_re = re.compile(
                f'.*/{subj_id}_{tp}' +
                r'_(?P<month>\d\d?)_(?P<day>\d\d?)_(?P<year>\d{4})' +
                r'(?:NEW )?(?:Remote *)?WISC(?:-V)?' +
                r'(?: *\(Core 10\))?(?: Part ?2)(?: New)?_information.txt')
        info_matches = [m for m in [info_file_re.fullmatch(str(f.resolve()))
                                    for f in scores_path.parent.iterdir()] if m]

        # More/less than 1 WISC info file? Error
        if len(info_matches) != 1:
            errors.append(QInteractiveUploaderError(
                    f'Expecting 1 WISC info file, got {len(info_matches)}.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        info_path = Path(info_matches[0].group(0))
        subj_report = QInteractiveReport(scores_path, info_path)

        # Check the info file scheduled assessment date against the file name
        path_date = datetime(int(info_matches[0].group('year')),
                             int(info_matches[0].group('month')),
                             int(info_matches[0].group('day'))
                            ).strftime('%Y-%m-%d')
        info_date = subj_report.extract_info(date_mapping)['wisc_date']
        if info_date != path_date:
            errors.append(QInteractiveUploaderError(
                    f'Assessment date in info file does not match path date.',
                    subj_id=subj_id, event=event, form_id='wisc',
                    form_path=scores_path))
            continue

        reports[(subj_id, event)] = subj_report

    if not reports:
        wisc_upl = None
    else:
        wisc_upl = QInteractiveUploader(reports, template_path, api_url, token,
                                        log_path, info_mappings=date_mapping)
    return wisc_upl, errors


def make_ktea_uploader(matches, template_path, api_url, token, log_path):
    errors = []
    reports = {}

    # Function to convert date from KTEA info file to a nice REDCap format
    date_fn = lambda d: datetime.strptime(d, '%B %d %Y').strftime('%Y-%m-%d')
    date_mapping = {'Scheduled Assessment Date': ('ktea_date', date_fn)}

    for (subj_id, event), report_matches in matches.items():
        # More than 1 KTEA form? Error
        if len(report_matches) != 1:
            errors += [QInteractiveUploaderError(
                            f'Expecting 1 report, got {len(report_matches)}.',
                            subj_id=subj_id, event=event, form_id='ktea',
                            form_path=report_match.group(0))
                       for report_match in report_matches]
            continue
        
        scores_path = Path(report_matches[0].group(0))

        # Find info file
        tp = event.split('_')[1]
        info_file_re = re.compile(
                f'.*/{subj_id}_{tp}' +
                r'_(?P<month>\d\d?)_(?P<day>\d\d?)_(?P<year>\d{4})(?:Remote )?'
                r'KTEA(?: \(BA-3\))?(?: Part\d)?_information.txt')
        info_matches = [m for m in [info_file_re.fullmatch(str(f.resolve()))
                                    for f in scores_path.parent.iterdir()] if m]

        # More/less than 1 KTEA info file? Error
        if len(info_matches) != 1:
            errors.append(QInteractiveUploaderError(
                    f'Expecting 1 KTEA info file, got {len(info_matches)}.',
                    subj_id=subj_id, event=event, form_id='ktea',
                    form_path=scores_path))
            continue

        info_path = Path(info_matches[0].group(0))
        subj_report = QInteractiveReport(scores_path, info_path)

        # Check the info file scheduled assessment date against the file name
        path_date = datetime(int(info_matches[0].group('year')),
                             int(info_matches[0].group('month')),
                             int(info_matches[0].group('day'))
                            ).strftime('%Y-%m-%d')
        info_date = subj_report.extract_info(date_mapping)['ktea_date']
        if info_date != path_date:
            errors.append(QInteractiveUploaderError(
                    f'Assessment date in info file does not match path date.',
                    subj_id=subj_id, event=event, form_id='ktea',
                    form_path=scores_path))
            continue

        reports[(subj_id, event)] = subj_report

    if not reports:
        ktea_upl = None
    else:
        ktea_upl = QInteractiveUploader(reports, template_path, api_url, token,
                                        log_path, info_mappings=date_mapping)
    return ktea_upl, errors


def make_dkefs_uploader(matches, template_path, api_url, token, log_path):
    dkefs_forms = ('CWI', 'DF', 'Sorting', 'Tower', 'TM', 'TQ', 'VF', 'WC')
    errors = []
    reports = {}

    for (subj_id, event), report_matches in matches.items():
        subj_report = DkefsReport()
        for report_match in report_matches:
            form_type = report_match.group('form')
            report_path = Path(report_match.group(0))
            if form_type not in dkefs_forms:
                errors.append(DkefsUploaderError(
                        f'Unknown form type {form_type}.', subj_id=subj_id,
                        event=event, form_id=f'DKEFS', form_path=report_path))
                break

            if subj_report.has_form(form_type):
                errors.append(DkefsUploaderError(
                        f'More than one scoring file for form {form_type}.',
                        subj_id=subj_id, event=event,
                        form_id=f'DKEFS_{form_type}', form_path=report_path))
                break

            score_values = report_path.read_text().strip().split(',')
            if score_values[0] == 'N':
                score_values[0] = score_values[1]
                score_values[1] = ''
                report_path.write_text(','.join(score_values))

            subj_report.add_report(report_path, form_type)

        if subj_report.reports:
            reports[(subj_id, event)] = subj_report

    if not reports:
        dkefs_upl = None
    else:
        dkefs_upl = DkefsUploader(reports, template_path, api_url, token,
                                  log_path, date_fields=['dkefs_date'])
    return dkefs_upl, errors


def make_qglobal_uploader(matches, template_path, api_url, token, log_path):
    reports = {}
    errors = []
    date_fields = ('vineland_date', 'basc_prs_date', 'basc_srp_date')

    for (subj_id, event), report_matches in matches.items():
        if len(report_matches) > 1:
            errors.append(QGlobalUploaderError(
                    'More than one scoring file.', subj_id=subj_id, event=event,
                    form_id=f'qglobal'))
        report_path = Path(report_matches[0].group(0))
        reports[(subj_id, event)] = QGlobalReport(report_path)

    if not reports:
        qg_upl = None
    else:
        qg_upl = QGlobalUploader(reports, template_path, api_url, token,
                                 log_path, date_fields=date_fields)

    return qg_upl, errors


def make_cpt3_uploader(matches, template_path, api_url, token, log_path):
    reports = {}
    errors = []

    for (subj_id, event), report_matches in matches.items():
        if len(report_matches) > 1:
            errors.append(CptUploaderError(
                    'More than one scoring file.', subj_id=subj_id, event=event,
                    form_id=f'cpt3'))
        report_path = Path(report_matches[0].group(0))
        reports[(subj_id, event)] = CptReport(report_path)

    if not reports:
        cpt_upl = None
    else:
        cpt_upl = CptUploader(reports, template_path, api_url, token, log_path,
                             date_fields=('cpt3_date', ))

    return cpt_upl, errors


def bgap_upload(base_path):
    print('\n====================')
    print('BGAP REDCap Uploader')
    print('====================')

    data_path = base_path / 'ParticipantFiles'
    redcap_path = base_path / 'REDCapUploads'
    token = (redcap_path / 'token.txt').read_text().rstrip('\r\n ')
    api_url = 'https://redcap.stanford.edu/api/'
    log_dir = redcap_path / 'logs'

    report_handlers = {
            # 'CPT-3':       (re.compile(r'.*[/\\]CPT3_Export_(?P<id>\d+)_'
            #                            r'(?P<tp>\d).xls'),
            #                 make_cpt3_uploader, 'bgap_cpt_template.xls'),
            # 'DKEFS':       (re.compile(r'.*[/\\]DKEFS_(?P<form>\w+)_(?P<id>\d+)_'
            #                            r'(?P<tp>\d).txt'),
            #                 make_dkefs_uploader, 'bgap_dkefs_template.csv'),
            'NIH Toolbox': (re.compile(r'.*[/\\]NIHTB_Scores_(?P<id>\d+)_(?P<tp>\d)'
                                       r'(?:_Remote)?.csv'),
                            make_nihtb_uploader, 'bgap_nihtb_template.csv'),
            'WISC-V':      (re.compile(r'.*[/\\]WISC[-_]V_Export_(?P<id>\d+)_'
                                       r'(?P<tp>\d).csv'),
                            make_wisc_uploader0, 'bgap_wiscv_template.csv'),
            'WISC-V-Part1':      (re.compile(r'.*[/\\]WISC[-_]V_Export_(?P<id>\d+)_'
                                       r'(?P<tp>\d)_Part1.csv'),
                            make_wisc_uploader1, 'bgap_wiscv_template.csv'),
            'WISC-V-Part2':      (re.compile(r'.*[/\\]WISC[-_]V_Export_(?P<id>\d+)_'
                                       r'(?P<tp>\d)_Part2.csv'),
                            make_wisc_uploader2, 'bgap_wiscv_template.csv'),
            'KTEA':        (re.compile(r'.*[/\\]KTEA\(BA-3\)_Export_(?P<id>\d+)_'
                                       r'(?P<tp>\d)(?:_Remote|_Part\d)?.csv'),
                            make_ktea_uploader, 'bgap_ktea_template.csv'),
            'Vineland':    (re.compile(r'.*[/\\]Vineland3_Report_(?P<id>\d+)_'
                                       r'(?P<tp>\d)\.docx?'),
                            make_qglobal_uploader,
                            'bgap_vineland_template.csv'),
            'BASC3-PRS':   (re.compile(r'.*[/\\]BASC3PRS_Report_(?P<id>\d+)_'
                                       r'(?P<tp>\d)\.docx?'),
                            make_qglobal_uploader,
                            'bgap_basc3prs_template.csv'),
            'BASC3-SRP':   (re.compile(r'.*[/\\]BASC3SRP_Report_(?P<id>\d+)_'
                                       r'(?P<tp>\d)\.docx?'),
                            make_qglobal_uploader,
                            'bgap_basc3srp_template.csv'),
    }

    print(f'Crawling over {data_path} for reports...\n')
    report_matches, errors = bgap_crawl(data_path, report_handlers)

    print(f'\n\nCreating uploaders...\n')
    for report_type, matches in report_matches.items():
        print(f'\nCreating uploader for {report_type}...')
        (_, uploader_maker, template_filename) = report_handlers[report_type]
        template_path = redcap_path / template_filename
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        log_path = log_dir / f'{report_type}_push_log_{timestamp}.txt'
        uploader, path_errors = uploader_maker(
                matches, template_path, api_url, token, log_path)
        errors += path_errors

        if uploader is None:
            continue

        print(f'Parsing and pushing scores for {report_type}...\n')
        subjs_events, response, push_errors = uploader.push()
        errors += push_errors

        if subjs_events:
            print(f'\n\nPushed data for {len(subjs_events)} records:')
            for subj, event in subjs_events:
                print(f'{subj}, {event}')

    if errors:
        print('\n\nSkipped the following records due to errors:')
        for err in errors:
            print(err)


def main():
    # bgap_upload(Path('/Volumes/Projects/KSTRT/Data'))
    bgap_upload(Path(r"C:\Users\yanghyun\Desktop\Stanford\CIBSR\FS REDCap Upload\KSTRT\Data"))


if __name__ == '__main__':
    main()

class BgapCrawlerError(Exception):
    pass
