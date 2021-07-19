# Script Dependencies
# Please execute the following command to install project dependencies
#    pip3 install requests-futures bs4 lxml
import concurrent
import csv
import re
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup
from lxml import html
import time
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
csv_writer_lock = threading.Lock()

BASE_URL = 'https://search.txcourts.gov/CaseSearch.aspx?coa=coa03&s=c'
OUTPUT_FILE_NAME = 'OUTPUT - ' + time.strftime("%Y-%m-%d-%H-%M-%S") + '.csv'
START_TIME = time.strftime("%Y-%m-%d-%H-%M-%S")
END_TIME = None
TOTAL_CASES_SCRAPPED = 0
TOTAL_UNIQUE_BAR_NUMBERS = 0

FILE_HEADER = ['BAR NUMBER',
               'CASE',
               'DATE FILED',
               'CASE TYPE',
               'STYLE',
               'V',
               'ORIG PROC',
               'TRANSFER FROM',
               'TRANSFER IN',
               'TRANSFER CASE',
               'TRANSFER TO',
               'TRANSFER OUT',
               'PUB SERVICE',
               'APPELLATE BRIEFS (DATE)',
               'APPELLATE BRIEFS (EVENT TYPE)',
               'APPELLATE BRIEFS (DESCRIPTION)',
               'APPELLATE BRIEFS (REMARKS)',
               'APPELLATE BRIEFS (DOCUMENT NAME)',
               'APPELLATE BRIEFS (DOCUMENT URL)',
               'APPELLATE BRIEFS (DOCUMENT DESCRIPTION)',
               'CASE EVENTS (DATE)',
               'CASE EVENTS (TYPE)',
               'CASE EVENTS (DISPOSITION)',
               'CASE EVENTS (REMARKS)',
               'CASE EVENTS (DOCUMENT NAME)',
               'CASE EVENTS (DOCUMENT URL)',
               'CASE EVENTS (DOCUMENT DESCRIPTION)',
               'CALENDAR (DATE)',
               'CALENDAR (TYPE)',
               'CALENDAR (REASON SET)',
               'CALENDAR (REMARKS)',
               'PARTIES (PARTY)',
               'PARTIES (PARTY TYPE)',
               'PARTIES (PARTY REPRESENTATIVE)',
               'TRIAL COURT',
               'TRIAL COURT COUNTRY',
               'TRIAL COURT JUDGE',
               'TRIAL COURT CASE',
               'TRIAL COURT REPORTER',
               'TRIAL COURT PUNISHMENT'
               ]


def extract_case_data(url):
    print('Extracting data from ', url)
    session,url_response = get(url)
    soup = BeautifulSoup(url_response.content, 'lxml')
    tree = html.fromstring(url_response.content)

    try:
        CASE = tree.xpath('//*[@id="case"]/div[2]/div/strong/text()')[0].strip()
    except:
        CASE = ''

    try:
        DATE_FILED = tree.xpath('//*[@id="panelTextSelection"]/div/div[2]/div[2]/div/text()')[0].strip()
    except:
        DATE_FILED = ''

    try:
        CASE_TYPE = tree.xpath('//*[@id="panelTextSelection"]/div/div[3]/div[2]/text()')[0].strip()
    except:
        CASE_TYPE = ''

    try:
        STYLE = tree.xpath('//*[@id="panelTextSelection"]/div/div[4]/div[2]/text()')[0].strip()
    except:
        STYLE = ''

    try:
        V = tree.xpath('//*[@id="panelTextSelection"]/div/div[5]/div[2]/text()')[0].strip()
    except:
        V = ''

    try:
        ORIG_PROC = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[1]/div[2]/text()')[0].strip()
    except:
        ORIG_PROC = ''

    try:
        TRANSFER_FROM = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[2]/div[2]/text()')[0].strip()
    except:
        TRANSFER_FROM = ''

    try:
        TRANSFER_IN = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[3]/div[2]/text()')[0].strip()
    except:
        TRANSFER_IN = ''

    try:
        TRANSFER_CASE = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[4]/div[2]/text()')[0].strip()
    except:
        TRANSFER_CASE = ''

    try:
        TRANSFER_TO = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[5]/div[2]/text()')[0].strip()
    except:
        TRANSFER_TO = ''

    try:
        TRANSFER_OUT = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[6]/div[2]/text()')[0].strip()
    except:
        TRANSFER_OUT = ''

    try:
        PUB_SERVICE = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_COAOnly"]/div[7]/div[2]/text()')[0].strip()
    except:
        PUB_SERVICE = ''

    # Working on APPELLATE BRIEFS
    try:
        appellate_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_grdBriefs_ctl00'})
        appellate_table_row = appellate_table.tbody.find_all('tr', {
            "id": re.compile('ctl00_ContentPlaceHolder1_grdBriefs_ctl00.*')})
    except:
        appellate_table_row = []

    APPELLATE_BRIEFS_DATE = ''
    APPELLATE_BRIEFS_EVENT_TYPE = ''
    APPELLATE_BRIEFS_DESCRIPTION = ''
    APPELLATE_BRIEFS_REMARKS = ''
    APPELLATE_BRIEFS_DOCUMENT_NAME = ''
    APPELLATE_BRIEFS_DOCUMENT = ''
    APPELLATE_BRIEFS_DOCUMENT_NOTE = ''
    CASE_EVENTS_DATE = ''
    CASE_EVENTS_TYPE = ''
    CASE_EVENTS_DISPOSITION = ''
    CASE_EVENTS_REMARKS = ''
    CASE_EVENTS_DOCUMENT_NAME = ''
    CASE_EVENTS_DOCUMENT = ''
    CASE_EVENTS_DOCUMENT_NOTE = ''
    CALENDAR_DATE = ''
    CALENDAR_TYPE = ''
    CALENDAR_REASON_SET = ''
    CALENDAR_REMARKS = ''
    PARTIES_PARTY = ''
    PARTIES_PARTY_TYPE = ''
    PARTIES_PARTY_REPRESENTATIVE = ''
    for row in appellate_table_row:
        try:
            APPELLATE_BRIEFS_DATE += row.td.text.strip()
        except:
            APPELLATE_BRIEFS_DATE += ''

        try:
            APPELLATE_BRIEFS_EVENT_TYPE += row.contents[2].text.strip()
        except:
            APPELLATE_BRIEFS_EVENT_TYPE += ''

        try:
            APPELLATE_BRIEFS_DESCRIPTION += row.contents[3].text.strip() + ''
        except:
            APPELLATE_BRIEFS_DESCRIPTION += ''

        if len(row.contents) > 6:
            try:
                APPELLATE_BRIEFS_REMARKS += row.contents[4].text.strip() + ''
            except:
                APPELLATE_BRIEFS_REMARKS += ''

        try:
            docGridTable = row.find('table', {'class': 'docGrid'})
            docGridTableRows = docGridTable.find_all('tr')

            for docGridTableRow in docGridTableRows:
                try:
                    docGridTableRowLink = 'https://search.txcourts.gov/' + docGridTableRow.a['href']
                    APPELLATE_BRIEFS_DOCUMENT += docGridTableRowLink + ''
                except:
                    APPELLATE_BRIEFS_DOCUMENT += ''

                try:
                    APPELLATE_BRIEFS_DOCUMENT_NAME += docGridTableRow.a.text.strip() + ''
                except:
                    APPELLATE_BRIEFS_DOCUMENT_NAME += ''

                try:
                    docGridTableRowText = docGridTableRow.td.next_sibling.text.strip()
                    APPELLATE_BRIEFS_DOCUMENT_NOTE += docGridTableRowText + ''
                except:
                    APPELLATE_BRIEFS_DOCUMENT_NOTE += ''

                if docGridTableRow != docGridTableRows[-1]:
                    APPELLATE_BRIEFS_DOCUMENT += '\e'
                    APPELLATE_BRIEFS_DOCUMENT_NAME += '\e'
                    APPELLATE_BRIEFS_DOCUMENT_NOTE +=  '\e'

        except:
            APPELLATE_BRIEFS_DOCUMENT += ''
            APPELLATE_BRIEFS_DOCUMENT_NAME += ''
            APPELLATE_BRIEFS_DOCUMENT_NOTE += ''

        if row != appellate_table_row[-1]:
            APPELLATE_BRIEFS_DATE += '\m'
            APPELLATE_BRIEFS_EVENT_TYPE += '\m'
            APPELLATE_BRIEFS_DESCRIPTION += '\m'
            APPELLATE_BRIEFS_REMARKS += '\m'
            APPELLATE_BRIEFS_DOCUMENT += '\m'
            APPELLATE_BRIEFS_DOCUMENT_NAME += '\m'
            APPELLATE_BRIEFS_DOCUMENT_NOTE += '\m'


    # Working on CASE EVENTS
    try:
        case_events_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_grdEvents_ctl00'})
        case_events_table_row = case_events_table.tbody.find_all('tr', {
            "id": re.compile('ctl00_ContentPlaceHolder1_grdEvents_ctl00.*')})
    except:
        case_events_table_row = []
    for row in case_events_table_row:
        try:
            CASE_EVENTS_DATE += row.td.text.strip()
        except:
            CASE_EVENTS_DATE += ''

        try:
            CASE_EVENTS_TYPE += row.contents[2].text.strip()
        except:
            CASE_EVENTS_TYPE += ''

        try:
            CASE_EVENTS_DISPOSITION += row.contents[3].text.strip()
        except:
            CASE_EVENTS_DISPOSITION += ''

        if len(row.contents) > 6:
            try:
                CASE_EVENTS_REMARKS += row.contents[4].text.strip()
            except:
                CASE_EVENTS_REMARKS += ''

        try:
            docGridTable = row.find('table', {'class': 'docGrid'})
            docGridTableRows = docGridTable.find_all('tr')

            for docGridTableRow in docGridTableRows:
                try:
                    docGridTableRowLink = 'https://search.txcourts.gov/' + docGridTableRow.a['href']
                    CASE_EVENTS_DOCUMENT += docGridTableRowLink
                except:
                    CASE_EVENTS_DOCUMENT += ''

                try:
                    CASE_EVENTS_DOCUMENT_NAME += docGridTableRow.a.text.strip()
                except:
                    CASE_EVENTS_DOCUMENT_NAME += ''

                try:
                    docGridTableRowText = docGridTableRow.td.next_sibling.text.strip()
                    CASE_EVENTS_DOCUMENT_NOTE += docGridTableRowText
                except:
                    CASE_EVENTS_DOCUMENT_NOTE += ''

                if docGridTableRow != docGridTableRows[-1]:
                    CASE_EVENTS_DOCUMENT += '\e'
                    CASE_EVENTS_DOCUMENT_NAME += '\e'
                    CASE_EVENTS_DOCUMENT_NOTE +=  '\e'
        except:
            CASE_EVENTS_DOCUMENT += ''
            CASE_EVENTS_DOCUMENT_NAME += ''
            CASE_EVENTS_DOCUMENT_NOTE += ''

        if row != case_events_table_row[-1]:
            CASE_EVENTS_DATE += '\m'
            CASE_EVENTS_TYPE += '\m'
            CASE_EVENTS_DISPOSITION += '\m'
            CASE_EVENTS_REMARKS += '\m'
            CASE_EVENTS_DOCUMENT += '\m'
            CASE_EVENTS_DOCUMENT_NAME += '\m'
            CASE_EVENTS_DOCUMENT_NOTE += '\m'
    # Working on calendars
    try:
        calendars_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_grdCalendar_ctl00'})
        calendars_table_row = calendars_table.tbody.find_all('tr', {
            "id": re.compile('ctl00_ContentPlaceHolder1_grdCalendar_ctl00.*')})
    except:
        calendars_table_row = []

    for row in calendars_table_row:
        try:
            CALENDAR_DATE += row.td.text.strip()
        except:
            CALENDAR_DATE += ''

        try:
            CALENDAR_TYPE += row.contents[2].text.strip()
        except:
            CALENDAR_TYPE += ''

        try:
            CALENDAR_REASON_SET += row.contents[3].text.strip()
        except:
            CALENDAR_REASON_SET += ''

        if len(row.contents) > 5:
            try:
                CALENDAR_REMARKS += row.contents[4].text.strip()
            except:
                CALENDAR_REMARKS += ''

        if row != calendars_table_row[-1]:
            CALENDAR_DATE += '\m'
            CALENDAR_TYPE += '\m'
            CALENDAR_REASON_SET += '\m'
            CALENDAR_REMARKS += '\m'

    # Working on Parties
    try:
        parties_table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_grdParty_ctl00'})
        parties_table_row = parties_table.tbody.find_all('tr', {
            "id": re.compile('ctl00_ContentPlaceHolder1_grdParty_ctl00.*')})
    except:
        parties_table_row = []

    for row in parties_table_row:
        try:
            PARTIES_PARTY += row.td.text.strip()
        except:
            PARTIES_PARTY += ''

        try:
            PARTIES_PARTY_TYPE += row.contents[2].text.strip()
        except:
            PARTIES_PARTY_TYPE += ''

        try:
            allrep = row.contents[3].get_text(separator="<br>")
            for rep in allrep.split("<br>"):
                PARTIES_PARTY_REPRESENTATIVE += rep.strip()
                if rep != allrep.split("<br>")[-1]:
                    PARTIES_PARTY_REPRESENTATIVE += '\e'

        except:
            PARTIES_PARTY_REPRESENTATIVE += ''

        if row != parties_table_row[-1]:
            PARTIES_PARTY += '\m'
            PARTIES_PARTY_TYPE += '\m'
            PARTIES_PARTY_REPRESENTATIVE += '\m'


    try:
        TRIAL_COURT = tree.xpath('//*[@id="panelTrialCourtInfo"]/div[2]/div[1]/div[2]/text()')[0].strip()
    except:
        TRIAL_COURT = ''

    try:
        TRIAL_COURT_COUNTRY = tree.xpath('//*[@id="panelTrialCourtInfo"]/div[2]/div[2]/div[2]/text()')[0].strip()
    except:
        TRIAL_COURT_COUNTRY = ''

    try:
        TRIAL_COURT_JUDGE = tree.xpath('//*[@id="panelTrialCourtInfo"]/div[2]/div[3]/div[2]/text()')[0].strip()
    except:
        TRIAL_COURT_JUDGE = ''

    try:
        TRIAL_COURT_CASE = tree.xpath('//*[@id="panelTrialCourtInfo"]/div[2]/div[4]/div[2]/text()')[0].strip()
    except:
        TRIAL_COURT_CASE = ''

    try:
        TRIAL_COURT_REPORTER = tree.xpath('//*[@id="panelTrialCourtInfo"]/div[2]/div[5]/div[2]/text()')[0].strip()
    except:
        TRIAL_COURT_REPORTER = ''

    try:
        TRIAL_COURT_PUNISHMENT = tree.xpath('//*[@id="ctl00_ContentPlaceHolder1_trPunishment"]/div[2]/text()')[
            0].strip()
    except:
        TRIAL_COURT_PUNISHMENT = ''

    # print('CASE', CASE)
    # print('DATE_FILED', DATE_FILED)
    # print('CASE_TYPE', CASE_TYPE)
    # print('STYLE', STYLE)
    # print('V', V)
    # print('ORIG PROC', ORIG_PROC)
    # print('TRANSFER FROM', TRANSFER_FROM)
    # print('TRANSFER_IN', TRANSFER_IN)
    # print('TRANSFER_CASE', TRANSFER_CASE)
    # print('TRANSFER_TO', TRANSFER_TO)
    # print('TRANSFER_OUT', TRANSFER_OUT)
    # print('PUB_SERVICE', PUB_SERVICE)
    # print('APPELLATE BREIFS DATE', APPELLATE_BRIEFS_DATE)
    # print('APPELLATE BRIEFS EVENT TYPE', APPELLATE_BRIEFS_EVENT_TYPE)
    # print('APPELLATE BRIEFS DESCRIPTION', APPELLATE_BRIEFS_DESCRIPTION)
    # print('APPELLATE_BRIEFS_DOCUMENT', APPELLATE_BRIEFS_DOCUMENT)
    # print('APPELLATE_BRIEFS_DOCUMENT_NOTE', APPELLATE_BRIEFS_DOCUMENT_NOTE)
    # print('APPELLATE_BRIEFS_REMARKS', APPELLATE_BRIEFS_REMARKS)
    # print('CASE_EVENTS_DATE', CASE_EVENTS_DATE)
    # print('CASE_EVENTS_TYPE', CASE_EVENTS_TYPE)
    # print('CASE_EVENTS_DISPOSITION', CASE_EVENTS_DISPOSITION)
    # print('CASE_EVENTS_REMARKS', CASE_EVENTS_REMARKS)
    # print('CASE_EVENTS_DOCUMENT_NOTE', CASE_EVENTS_DOCUMENT_NOTE)
    # print('CASE_EVENTS_DOCUMENT', CASE_EVENTS_DOCUMENT)
    # print('CALENDAR_DATE', CALENDAR_DATE)
    # print('CALENDAR_TYPE', CALENDAR_TYPE)
    # print('CALENDAR_REASON_SET', CALENDAR_REASON_SET)
    # print('CALENDAR_REMARKS', CALENDAR_REMARKS)
    # print('PARTIES_PARTY',PARTIES_PARTY)
    # print('PARTIES_PARTY_TYPE',PARTIES_PARTY_TYPE)
    # print('PARTIES_PARTY_REPRESENTATIVE',PARTIES_PARTY_REPRESENTATIVE)
    # print('TRIAL_COURT',TRIAL_COURT)
    # print('TRIAL_COURT_COUNTRY',TRIAL_COURT_COUNTRY)
    # print('TRIAL_COURT_JUDGE',TRIAL_COURT_JUDGE)
    # print('TRIAL_COURT_CASE',TRIAL_COURT_CASE)
    # print('TRIAL_COURT_REPORTER',TRIAL_COURT_REPORTER)
    # print('TRIAL_COURT_PUNISHMENT',TRIAL_COURT_PUNISHMENT)

    return [CASE,
            DATE_FILED,
            CASE_TYPE,
            STYLE,
            V,
            ORIG_PROC,
            TRANSFER_FROM,
            TRANSFER_IN,
            TRANSFER_CASE,
            TRANSFER_TO,
            TRANSFER_OUT,
            PUB_SERVICE,
            APPELLATE_BRIEFS_DATE,
            APPELLATE_BRIEFS_EVENT_TYPE,
            APPELLATE_BRIEFS_DESCRIPTION,
            APPELLATE_BRIEFS_REMARKS,
            APPELLATE_BRIEFS_DOCUMENT_NAME,
            APPELLATE_BRIEFS_DOCUMENT,
            APPELLATE_BRIEFS_DOCUMENT_NOTE,
            CASE_EVENTS_DATE,
            CASE_EVENTS_TYPE,
            CASE_EVENTS_DISPOSITION,
            CASE_EVENTS_REMARKS,
            CASE_EVENTS_DOCUMENT_NAME,
            CASE_EVENTS_DOCUMENT,
            CASE_EVENTS_DOCUMENT_NOTE,
            CALENDAR_DATE,
            CALENDAR_TYPE,
            CALENDAR_REASON_SET,
            CALENDAR_REMARKS,
            PARTIES_PARTY,
            PARTIES_PARTY_TYPE,
            PARTIES_PARTY_REPRESENTATIVE,
            TRIAL_COURT,
            TRIAL_COURT_COUNTRY,
            TRIAL_COURT_JUDGE,
            TRIAL_COURT_CASE,
            TRIAL_COURT_REPORTER,
            TRIAL_COURT_PUNISHMENT
        ]


def get(url):
    tries = 0
    url_response = None
    req = FuturesSession()
    while True:
        try:
            url_response_future = req.get(url,timeout=40)
            url_response = url_response_future.result()
            if url_response.status_code != 200:
                raise Exception('response not 200')
            break
        except:
            if tries == 3:
                print(f'SKIPPING {url}')
                break
            print(f"Retrying url {url}")
            time.sleep(tries * 3)
            tries += 1
    return req,url_response


def post(req,url,data):
    tries = 0
    url_response = None
    while True:
        try:
            url_response_future = req.post(url,data=data,timeout=30)
            url_response = url_response_future.result()
            if url_response.status_code != 200:
                raise Exception('response not 200')
            break
        except Exception as e:
            if tries == 3:
                print(f'SKIPPING {url}')
                break
            print(f"Retrying url {url}")
            time.sleep(tries * 3)
            tries += 1
    return url_response
def scroll_by_bar_number(barNumber):
    print(f'================== Working on bar number {barNumber} ======================')
    total_count = 0
    # session,url_response = get(BASE_URL)
    # soup = BeautifulSoup(url_response.content, 'lxml')
    viewstate = None

    data = {
        '__EVENTTARGET': '',
        '__VIEWSTATEGENERATOR': '64154690',
        '__SCROLLPOSITIONX': '0',
        '__SCROLLPOSITIONY': '120',
        'ctl00$ContentPlaceHolder1$SearchType': 'rbSearchByCase',
        'ctl00$ContentPlaceHolder1$chkAllCourts': 'on',
        'ctl00$ContentPlaceHolder1$ddlCourts': '2f9a4941-9b55-463d-a622-b6b304b19142',
        'ctl00$ContentPlaceHolder1$txtCaseNumber': '',
        'ctl00$ContentPlaceHolder1$txtPartialCaseNumber': '',
        'ctl00$ContentPlaceHolder1$olCaseType': '0',
        'ctl00$ContentPlaceHolder1$txtDateFiledStart': '',
        'ctl00$ContentPlaceHolder1$txtDateFiledStart$dateInput': '',
        'ctl00_ContentPlaceHolder1_txtDateFiledStart_dateInput_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","minDateStr":"1900-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00","lastSetTextBoxValue":""}',
        'ctl00_ContentPlaceHolder1_txtDateFiledStart_calendar_SD': '[]',
        'ctl00_ContentPlaceHolder1_txtDateFiledStart_calendar_AD': '[[1900,1,1],[2099,12,30],[2021,7,12]]',
        'ctl00_ContentPlaceHolder1_txtDateFiledStart_ClientState': '{"minDateStr":"1900-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00"}',
        'ctl00$ContentPlaceHolder1$txtDateFiledEnd': '',
        'ctl00$ContentPlaceHolder1$txtDateFiledEnd$dateInput': '',
        'ctl00_ContentPlaceHolder1_txtDateFiledEnd_dateInput_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","minDateStr":"1900-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00","lastSetTextBoxValue":""}',
        'ctl00_ContentPlaceHolder1_txtDateFiledEnd_calendar_SD': '[]',
        'ctl00_ContentPlaceHolder1_txtDateFiledEnd_calendar_AD': '[[1900,1,1],[2099,12,30],[2021,7,12]]',
        'ctl00_ContentPlaceHolder1_txtDateFiledEnd_ClientState': '{"minDateStr":"1900-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00"}',
        'ctl00$ContentPlaceHolder1$txtStyle1': '',
        'ctl00$ContentPlaceHolder1$txtStyle2': '',
        'ctl00$ContentPlaceHolder1$txtAttorneyNameOrBarNumber': barNumber,
        'ctl00$ContentPlaceHolder1$txtTrialCourtCaseNumber': '',
        'ctl00$ContentPlaceHolder1$ddlOriginateCOA': '',
        'ctl00$ContentPlaceHolder1$ddCounty': '',
        'ctl00$ContentPlaceHolder1$ddlTrialCourt': '',
        'ctl00$ContentPlaceHolder1$btnSearch': 'Search',
        'ctl00$ContentPlaceHolder1$hdnCount': '',
        'ctl00$ContentPlaceHolder1$hdnMode': 'false'
    }

    next_page_btn = None
    last_item = None
    req = FuturesSession()
    while True:

        if next_page_btn is not None:
            data[next_page_btn] = ''

        data['__VIEWSTATE'] = viewstate
        url_response = post(req,BASE_URL,data)
        soup = BeautifulSoup(url_response.content, 'lxml')

        #checking for direct case
        if 'Case.aspx' in url_response.url:
            anchors = soup.find_all('a', href=re.compile(".*Case.aspx.*"))
        else:
            anchors = soup.find_all('a', href=re.compile(".*Case.aspx.*"), id=re.compile('ctl00_ContentPlaceHolder1_grdCases.*'))

        sorted_anchors = sorted(anchors, key=lambda x: x.text)
        if len(sorted_anchors) == 0 or last_item == sorted_anchors[-1].text:
            break
        else:
            last_item = sorted_anchors[-1].text

        if any("https://search.txcourts.gov" in a['href'] for a in anchors):
            anchors = [a for a in anchors if "https://search.txcourts.gov" in a['href']]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for a in anchors:
                if 'https://search.txcourts.gov' not in a['href']:
                    case_link = "https://search.txcourts.gov/" + a['href']
                else:
                    case_link = a['href']
                futures.append(executor.submit(extract_case_data,case_link))
            output_data = []
            for result in concurrent.futures.as_completed(futures):
                case_data = result.result()
                if case_data is not None:
                    total_count += 1
                    output_data.append([barNumber] + case_data)
            write_to_file(output_data)
        viewstate = soup.find('input', {'id': '__VIEWSTATE'})['value']

        try:
            next_page_btn = soup.find('input', {'title': 'Next Page'})['name']
            if next_page_btn is None:
                break
        except:
            break

        print(
            f"========================= Working for next page for bar number {barNumber} ==============================")
    print(f"============== Total cases scrapped from bar {barNumber} are {total_count} ================")
    return total_count,barNumber

def write_to_file(rows):
    global file,writer
    with csv_writer_lock:
        file = open(OUTPUT_FILE_NAME, 'a', encoding='UTF8' ,newline="")
        writer = csv.writer(file)
        writer.writerows(rows)
        file.close()


if __name__ == '__main__':
    start = time.time()

    write_to_file([FILE_HEADER])
    input_file_name = sys.argv[1]

    print(f'Trying to open {input_file_name}')

    bar_numbers = []
    total_cases = []
    with open(input_file_name, encoding='UTF8') as input_file:
        csv_reader = csv.reader(input_file, delimiter=',')
        header = True
        for row in csv_reader:
            if header:
                header = False
                continue
            bar_number = row[0]
            if bar_number not in bar_numbers:
                bar_numbers.append(bar_number)
                try:
                    total_case = row[1]
                except:
                    total_case = 0
                total_cases.append(total_case)
            else:
                print(bar_number)
    input_file.close()

    TOTAL_UNIQUE_BAR_NUMBERS = len(bar_numbers)

    with ThreadPoolExecutor(max_workers=200) as executor:
        futures = [executor.submit(scroll_by_bar_number, bar_number) for bar_number in bar_numbers]
        for result in concurrent.futures.as_completed(futures):
            tc,bn = result.result()
            TOTAL_CASES_SCRAPPED += tc
            if str(tc) != total_cases[bar_numbers.index(bn)]:
                print(f"DEBUG ============ Bar numer {bn} has more cases scrapped {tc} then actual {total_cases[bar_numbers.index(bn)]}")

    END_TIME = time.strftime("%Y-%m-%d-%H-%M-%S")
    end = time.time()

    print("=============== STATISTICS ===================")
    print("START TIME: ", START_TIME)
    print("ENDT TIME: ", END_TIME)
    print("TIME TAKEN (INCLUDES BOOT TIME): {:.6f}s".format(end-start))
    print("ACTAUL TIME TAKEN: {:.6f}s".format((end - start) * 4/5))
    print("TOTAL UNIQUE BAR NUMER IN INPUT: ", TOTAL_UNIQUE_BAR_NUMBERS)
    print("TOTAL CASES SCRAPPED: ",TOTAL_CASES_SCRAPPED)


# scroll_by_bar_number('14985000')
# scroll_by_bar_number('16705480')
# extract_case_data("https://search.txcourts.gov/Case.aspx?cn=04-20-00514-CV&coa=coa04")
# extract_case_data("https://search.txcourts.gov/Case.aspx?cn=02-1177&coa=cossup")
# extract_case_data('https://search.txcourts.gov/Case.aspx?cn=09-20-00196-CV&coa=coa09')
# extract_case_data('https://search.txcourts.gov/Case.aspx?cn=17-0271&coa=cossup')
# extract_case_data('https://search.txcourts.gov/Case.aspx?cn=03-00-00000-CV&coa=cossup')