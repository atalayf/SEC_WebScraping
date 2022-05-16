import os
import requests
import json
import pandas as pd
import time
import numpy as np
import random
import csv
import xml.etree.ElementTree as ET
from datetime import datetime



def get_data(link):
    """Gets the data from the link and returns the content"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
        'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36'
    ]
    user_agent = random.choice(user_agents)
    headers = {'User-Agent': user_agent}

    try:
        print(f'Parsing :{link}')
        if link[-3:] != 'xml':
            print('File error.')
            content = 'File error.'
        else:
            req = requests.get(link, headers=headers)
            if req.status_code == 200:
                content = req.content
            else:
                print('Error reading the page')
                retry_count = 1
                while retry_count < 4:
                    random_sleep(retry_count**2*15, retry_count**2*36)
                    req = requests.get(link, headers=headers)
                    if req.status_code == 200:
                        content = req.content
                        retry_count = 10
                    else:
                        print(req.status_code)
                        print(f'retry count: {retry_count}')
                        retry_count += 1
                # skip missing files and add to error log (until 11/17/21 only 3 Form-4 links gave 404 response code and they were missing from the server)
                if req.status_code == 404:
                    content = 'File error.'
    except:
        print('Please check your internet connection')
        quit()

    return content


def parse_xml_form4(xml):
    """Parses  xml and returns the non-derivative and derivative transaction tables as dataframes"""

    nderiv_df = pd.DataFrame()
    deriv_df = pd.DataFrame()

    tree = ET.ElementTree(ET.fromstring(xml))

    root = tree.getroot()

    deriv_table = root.find('derivativeTable')

    nderiv_table = root.find('nonDerivativeTable')

    if nderiv_table:
        table1_n_trans = len(nderiv_table)

    if deriv_table:
        table2_n_trans = len(deriv_table)

    owner_names = ['companyCik', 'cusip', 'companyName', 'documentType', 'rptOwnerCik', 'rptOwnerName', 'isDirector',
                   'isOfficer', 'isTenPercentOwner', 'isOther', 'officerTitle', 'signatureDate']
    table1_names = ['securityTitle', 'transactionDate', 'deemedExecutionDate', 'transactionCode',
                    'equitySwapInvolved', 'transactionShares', 'transactionAcquiredDisposedCode',
                    'transactionPricePerShare', 'sharesOwnedFollowingTransaction', 'directOrIndirectOwnership']
    table2_names = ['securityTitle', 'conversionOrExercisePrice', 'transactionDate', 'deemedExecutionDate',
                    'transactionCode', 'equitySwapInvolved', 'transactionShares', 'transactionPricePerShare',
                    'transactionAcquiredDisposedCode', 'exerciseDate', 'expirationDate', 'underlyingSecurityTitle',
                    'underlyingSecurityShares', 'sharesOwnedFollowingTransaction', 'directOrIndirectOwnership']

    table1_data_names = owner_names + table1_names
    table2_data_names = owner_names + table2_names

    def parse_table(names, table_name, n_trans, tree_root):
        
        # get the column names as the distinct table 1 and table 2 names

        names, ind = np.unique(np.array(names), return_index=True)
        names = names[np.argsort(ind)]
        names = names.tolist()

        # Get footnote and owner info.

        owner_info = tree_root.find('reportingOwner').iter()

        try:
            footnotes = tree_root.find('footnotes').iter()
            n_footnotes = len(tree_root.find('footnotes'))
        except:
            footnotes = ''
            n_footnotes = 0

        def footnoteNames(n):
            return ['F' + str(i + 1) for i in range(n)]

        names = names + [v + '_footnoteId' for v in names] + footnoteNames(n_footnotes)
        table_data = pd.DataFrame(columns=names)

        row = {}  # new row to be added to the dataframe

        # Populate owner info and footnotes, which are the same for every transaction.

        for col in table_data.columns:
            row[col] = None

        for o in owner_info:
            if o.tag in list(row.keys()):
                row[o.tag] = o.text

        for f in footnotes:
            if f.attrib.get('id') in list(row.keys()):
                row[f.attrib.get('id')] = f.text

        # Parse each transaction and save to new row.
        # First, parse the xml file and save each branch in a dictionary by itself to make it easier to parse.

        for i in range(n_trans):
            new_row = row.copy()
            table_item = []

            for j in tree_root.find(table_name)[i].iter():
                table_item.append({'tag': j.tag, 'attribute': j.attrib, 'text': j.text})

            for k in range(len(table_item)):
                item = table_item[k]
                tag = item['tag']
                text = item['text']
                if tag in list(new_row.keys()):
                    if text:
                        if text.strip() == '':
                            value_item = table_item[k + 1]
                            if value_item['attribute'] != {}:
                                new_row[tag] = value_item['attribute'].get('id')
                            else:
                                new_row[tag] = value_item['text']
                        else:
                            new_row[tag] = text
                elif tag == 'footnoteId':
                    tag_item = table_item[k - 2]
                    actual_tag = tag_item['tag'] + '_footnoteId'
                    if actual_tag in list(new_row.keys()):
                        new_row[actual_tag] = item['attribute'].get('id')

            table_data = table_data.append(new_row, ignore_index=True)

        table_data['companyCik'] = cik
        df_cik_cusip_title = pd.read_csv('cik_cusip_title.csv', converters={'cik': lambda x: str(x),
                                                                            'cusip': lambda x: str(x)})

        try:
            table_data['cusip'] = df_cik_cusip_title.loc[df_cik_cusip_title['cik'] == cik].values[0][0]
            table_data['companyName'] = df_cik_cusip_title.loc[df_cik_cusip_title['cik'] == cik].values[0][3]
        except:
            print('Company info not found in cik_cusip_title.csv file.')
            table_data['cusip'] = 'not found'
            table_data['companyName'] = 'not found'
            pass

        table_data['documentType'] = tree_root.find('documentType').text
        table_data['signatureDate'] = tree_root.find('ownerSignature').find('signatureDate').text

        date_cols = [col for col in table_data.columns if 'Date' in col]
        for col in date_cols:
            table_data[col] = table_data[col].str.replace('-', '')

        return table_data

    if nderiv_table:
        nderiv_df = parse_table(names=table1_data_names, table_name='nonDerivativeTable',
                                n_trans=table1_n_trans, tree_root=root)

    if deriv_table:
        deriv_df = parse_table(names=table2_data_names, table_name='derivativeTable',
                               n_trans=table2_n_trans, tree_root=root)
    return nderiv_df, deriv_df


def extract_form_4_list(file, start_date, end_date):
    """Reads json file and returns the list of form 4 transactions
    for submissions list we use https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip file.
    This file includes the list of all the prior submissions.
    For more information on the file format, please see https://www.sec.gov/edgar/sec-api-documentation
    They are saved in json format. names as CIK##########.json 10 digit cik number with leading zeros.
    We'll parse the files and create pandas dataframes.
    The extracted json submissions files should be saved in [base_path+'submissions'] folder.

    start_date and end_date are the start and end dates of the form 4 transactions. (start_date <= date <= end_date)

    """



    if '-submissions-' in file:
        # For companies with additional submission files. Additional files have a different json structure.
        with open(file, "r") as read_file:
            print("Converting JSON encoded data into Python dictionary")
            json_content = json.load(read_file)
        df = pd.DataFrame.from_dict(json_content, orient='columns')
        df_form_4 = df.loc[df['form'] == '4']
        # filter the dates
        df_form_4['filingDate'] = pd.to_datetime(df_form_4['filingDate'].astype(str).str.strip(), format='%Y-%m-%d')
        df_form_4 = df_form_4.loc[(df_form_4['filingDate'] >= start_date) & (df_form_4['filingDate'] <= end_date)]

    else:
        with open(file, "r") as read_file:
            print("Converting JSON encoded data into Python dictionary")
            json_content = json.load(read_file)
        df = pd.DataFrame.from_dict(json_content['filings']['recent'], orient='columns')
        df_form_4 = df.loc[df['form'] == '4']
        # filter the dates
        df_form_4['filingDate'] = pd.to_datetime(df_form_4['filingDate'].astype(str).str.strip(), format='%Y-%m-%d')
        df_form_4 = df_form_4.loc[(df_form_4['filingDate'] >= start_date) & (df_form_4['filingDate'] <= end_date)]

    accession_numbers = df_form_4['accessionNumber'].tolist()
    accession_numbers = [number.replace('-', '') for number in accession_numbers]

    file_name = df_form_4['primaryDocument'].tolist()
    file_name = [doc.split('/') for doc in file_name]
    file_name = [doc[-1] for doc in file_name]

    file_info = list(zip(accession_numbers, file_name))
    return file_info


def random_sleep(min, max):
    """Sleeps for a random time between min and max seconds"""
    time.sleep(random.randint(min, max) / 100)



base_path = r'C:\Users\atala\SEC'
save_directory = base_path + '\\tables\\'
#  start_date and end_date are the start and end dates of the form 4 transactions. (start_date <= date <= end_date)
start_date = '2012-05-01'
end_date = '2022-05-14'

start_date = datetime.strptime(start_date, '%Y-%m-%d')
end_date = datetime.strptime(end_date, '%Y-%m-%d')


# Get the list of all the files in the directory
all_submission_files = os.listdir(base_path + r'\submissions')

while all_submission_files:
    nderiv_transactions = pd.DataFrame()
    deriv_transactions = pd.DataFrame()
    error_log = []
    # get the first file for processing
    submission_file = all_submission_files.pop(0)
    # move, if the file is placeholder.txt file
    if submission_file[-4:] != 'json':
        os.rename(base_path + r'\submissions\\' + submission_file,
                  base_path + r'\submissions\processed\\' + submission_file)

    print(f'processing: {submission_file}')

    link_info = extract_form_4_list(base_path + r'\submissions\\' + submission_file, start_date, end_date)

    print(link_info)
    cik = submission_file[3:13]
    initial_number_of_forms = len(link_info)

    while len(link_info) > 0:
        accs_number, file_name = link_info.pop() 
        print(f'%{((1 - (len(link_info) / initial_number_of_forms)) * 100):.2f} processed.')
        print(f'cik: {cik}')
        print(f'accsNum: {accs_number}')
        url = f'https://www.sec.gov/Archives/edgar/data/{cik}/{accs_number}/{file_name}'
        xml_content = get_data(url)

        # Check if the file is valid.
        if xml_content == 'File error.':
            # add error log
            error_log.append(accs_number)
            pass
        else:
            # random_sleep(1, 4)
            nderiv_df, deriv_df = parse_xml_form4(xml_content)
            nderiv_transactions = pd.concat([nderiv_transactions, nderiv_df], ignore_index=True, sort=False)
            deriv_transactions = pd.concat([deriv_transactions, deriv_df], ignore_index=True, sort=False)
    
    # save the error log to csv files
    if error_log:
        with open(save_directory + submission_file[:-5] + "-file-errors.csv", 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for item in error_log:
                wr.writerow([item])

    # save the dataframes to csv files
    if nderiv_transactions.shape[0] > 0:
        # replace the commas in text to periods. 
        nderiv_transactions.replace(', ','. ', regex=True, inplace=True)
        #remove the commas in nummbers
        nderiv_transactions.replace(',', '', regex=True, inplace=True)

        nderiv_transactions.to_csv(save_directory + submission_file[:-5] + "-nderiv.csv")
    if deriv_transactions.shape[0] > 0:
        # replace the commas in text to periods.
        deriv_transactions.replace(', ','. ', regex=True, inplace=True)
        # remove the commas in nummbers
        deriv_transactions.replace(',', '', regex=True, inplace=True)

        deriv_transactions.to_csv(save_directory + submission_file[:-5] + "-deriv.csv")

    # move the processed file to 'processed' folder
    os.rename(base_path + r'\submissions\\' + submission_file,
              base_path + r'\submissions\processed\\' + submission_file)

    
