import os
import shutil
import re
import calendar
import sys
from datetime import datetime as dt
import pandas as pd
import numpy as np
import config
import pickle
from partnership_processing_toolbox import file_tools
import validation_tools
import phc_tools
###################################################################################################################

def extract_site(xls_file_name, site_dict):
    """
    :param xls_file_name: name of raw xls file
    :param full: if True return the full site to rename xls file, if False return short site name for csv file
    :return: site name in short or full context
    """
    site = None
    for site_id in site_dict:
        if str(site_id) in xls_file_name:
            site = site_dict[site_id]

    if site is None:
        print('Missing Site in Membership Site Dict' + xls_file_name)
        sys.exit(1)
    else:
        return site


def extract_month(xls_file_name):
    """
    :param xls_file_name: name of raw xls file
    :return: month number
    """
    # map full month name to month number
    for month in calendar.month_name[1:13]:
        if month in xls_file_name:
            month_number = list(calendar.month_name).index(month)
            return month_number


def extract_year(xls_file_name):
    """
    :param xls_file_name: name of raw xls file
    :return: year
    """
    return re.findall(r'.*(\d{4})\.xls$', xls_file_name)[0]


def rename_and_move_xls_file(unprocessed_dir, xls_file_path, site_dict):
    """
    :param unprocessed_dir: name of directory where files are located
    :param site_dict: dictionary to rename name of files
    :param xls_file_path: name of raw xls file
    :return: None
    https://stackoverflow.com/questions/7287996/python-get-relative-path-from-comparing-two-absolute-paths
    """
    file_folder_path = os.path.dirname(xls_file_path)
    clinic_key = extract_site(xls_file_path, site_dict)
    month = f'{extract_month(xls_file_path):02d}'
    year = extract_year(xls_file_path)
    config.membership_files_processed_directory.joinpath('PHP-Raw-Files').joinpath(year).mkdir(exist_ok=True)
    config.membership_files_processed_directory.joinpath('PHP-Raw-Files').joinpath(year).joinpath(month).mkdir(
        exist_ok=True)
    php_raw_file_dir = config.membership_files_processed_directory.joinpath('PHP-Raw-Files').joinpath(year).joinpath(
        month)

    name_split = re.split('-|_', os.path.basename(xls_file_path))
    renamed_xls_file = name_split[0] + '-' + clinic_key + '_' + name_split[2] + '_' + month + '-' + name_split[4]
    renamed_xls_file_path = rf'{file_folder_path}\{renamed_xls_file}'

    # rename xls file if in the proper format
    if re.fullmatch(r'^\w+-\w+_\d+_\d{2}-\d{4}\.xls$', os.path.basename(renamed_xls_file_path)):
        os.rename(xls_file_path, renamed_xls_file_path)
    else:
        return print('Cant rename xls file')

    shutil.move(renamed_xls_file_path, str(php_raw_file_dir.joinpath(renamed_xls_file)))


def format_phone_number(phone_number):
    """
    :param phone_number: phone number column in the raw file
    :return: clean version of the phone number
    """
    # remove all special characters
    number = re.sub(r'\W+', '', str(phone_number))

    if len(number) == 10:
        # format to 123-123-1234
        return re.sub(r'(\d{3})(\d{3})(\d{4})', '\\1-\\2-\\3', number)
    elif len(number) == 7:
        # format to 123-1234
        return re.sub(r'(\d{3})(\d{4})', '\\1-\\2', number)
    else:
        return ''


def format_full_address(records):
    address = format_address(records['Residential Address1'], records['Residential Address2'])

    city = np.where(records['Residential City'].isnull(), '', records['Residential City'])
    state = np.where(records['Residential State'].isnull(), '', records['Residential State'])
    zip_code = np.where(records['Residential Zip'].isnull(), "", records['Residential Zip'])

    full_address = (address + ' :: ' + city + ' ~ ' + state + ' | ' + zip_code).str.strip().str.replace("'", "''")

    return full_address


def format_address(address1, address2):
    """
    :param address1: Residential Address 1 column in xls file
    :param address2: Residential Address 2 column in xls file
    :return: full address of the member
    """
    address1 = address1.replace('-$', '')
    address2 = np.where((address2.isnull()) | (address2 == '-'), '', address2)

    address = (address1 + address2).str.strip().str.replace("'", "''")

    return address


def check_xls_fields(columns):
    """
    :param columns: all column names of an xls file
    :return: string of expected fields missing in the file
    """
    expected_fields = pd.Series(
        ['BIC#/HIK#', 'First Name', 'Last Name', 'Birth', 'Sex',
         'Record#', 'RP', 'Eff Date', 'End Date',
         'Other Insurnace', 'New Member',
         'Residential Address1', 'Residential Address2',
         'Residential City', 'Residential State',
         'Residential Zip', 'Residential Phone#'])

    # check if field names have not changed
    missing_fields = ~ expected_fields.isin(columns)

    if any(missing_fields):
        return print(expected_fields[missing_fields].str.cat(', '))


def make_membership_file(xls_files_list, site_dict):
    """
    :param xls_files_list: list of xls files that were grouped by site in dictionary
    :return: data frame of membership records grouped by site
    """
    membership_files_list = []

    for file in xls_files_list:
        # xls file is actually in XML format with html tags for a table
        df = pd.read_html(file)[0]

        # check for any field name changes before creating membership file
        check_xls_fields(df.columns)

        # create the membership file (this will reflect the Membership table in the SQL Server: PHP DB)
        membership_file = pd.DataFrame({
            'BIC_HIK': df['BIC#/HIK#'],
            'Clinic': extract_site(file, site_dict),
            'LastName': df['Last Name'].str.replace("'", "''"),
            'FirstName': df['First Name'].str.replace("'", "''"),
            'DOB': pd.to_datetime(df['Birth']),
            'Sex': df['Sex'].replace({'F': 'Female', 'M': 'Male'}),
            'PhoneNumber': df['Residential Phone#'].apply(format_phone_number),
            'Address': format_address(df['Residential Address1'], df['Residential Address2']),
            'City': df['Residential City'],
            'State': df['Residential State'],
            'ZipCode': df['Residential Zip'],
            'FullAddress': format_full_address(df),
            'RecordNumber': df['Record#'],
            'RP': df['RP'],
            'EffDate': pd.to_datetime(df['Eff Date']),
            'EndDate': pd.to_datetime(df['End Date']),
            'OtherInsurance': df['Other Insurnace'],
            'NewMember': np.where(df['New Member'] == '*', 'TRUE', 'FALSE'),
            'Month': 1,
            'PHPCurrentFlag': 1
        })

        # update Month column name to month abbreviation of the file
        month_abbr = calendar.month_abbr[extract_month(file)]
        membership_file.rename(columns={'Month': month_abbr}, inplace=True)

        # add SpecialMemberFlag and value depending on file name with _M or _S
        if 'Capitated' in extract_site(file, site_dict) or 'CCS' in extract_site(file, site_dict):
            membership_file['SpecialMemberFlag'] = 0
        else:
            membership_file['SpecialMemberFlag'] = 1

        # add Last Updated as final column based on the date it was downloaded
        membership_file['LastUpdated'] = pd.to_datetime(dt.fromtimestamp(os.path.getmtime(file)).strftime('%Y-%m-%d'))

        # add membership file to list to combine at the end
        membership_files_list.append(membership_file)

    return pd.concat(membership_files_list).drop_duplicates()


def make_xls_files_dict(xls_files_list, site_dict):
    """
    :param xls_files_list: list of all xls files from working directory
    :return: a dictionary with key as a csv file name to output and a list of xls file names grouped by site
    """
    xls_files_dict = {}

    for file in xls_files_list:
        short_site = extract_site(file, site_dict)
        month = extract_month(file)
        year = extract_year(file)

        # stop if file could not extract site, month, or year
        if None in [short_site, month, year]:
            return print('Stop')

        # key will be a .txt file name
        key = f'{short_site}_{month:02d}-{year}.txt'

        # add xls file to list based on key name
        if key not in xls_files_dict.keys():
            xls_files_dict[key] = [file]
        else:
            xls_files_dict[key].append(file)

    return xls_files_dict


try:
    if not os.path.isfile("./membership_download_success.pkl"):
        raise Exception
except Exception:
    error = 'Membership Download was not a success'
    print(error)
    sys.exit()

try:
    # connect to partnership database to query active clinic values
    phc_db_conn = phc_tools.SQL_connection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH)

    active_clinic_keys = phc_tools.active_values_query(db_connection=phc_db_conn,
                                                       return_type='list',
                                                       table_name='clinic_map',
                                                       query_col_1='clinic_key')
    phc_id_to_clinic_key_map = phc_tools.active_values_query(db_connection=phc_db_conn,
                                                             return_type='dict',
                                                             table_name='clinic_map',
                                                             query_col_1='phc_id',
                                                             query_col_2='clinic_key')

    phc_db_conn.close()
except:
    error = 'Failed querying active values from partnership database'
    print(error)
    phc_db_conn.close()
    sys.exit()

try:
    # set directory and get a list of xls files to process
    download_directory = config.membership_files_download_directory
    processed_directory = config.membership_files_processed_directory

    xls_files_list = file_tools.get_file_paths_list(str(download_directory), walk=True, files_extension='xls')

    # group xls files into dictionary based on site
    xls_files_dict = make_xls_files_dict(xls_files_list, phc_id_to_clinic_key_map)

    # read/process/rename/move xls files and output processed membership files
    for key, xls_files_list in xls_files_dict.items():
        # pass xls files list to read and make into one file
        membership_file = make_membership_file(xls_files_list, phc_id_to_clinic_key_map)

        # pass xls files to be renamed and moved
        for file in xls_files_list:
            rename_and_move_xls_file(str(download_directory), file, phc_id_to_clinic_key_map)

        # use name of key as file name for membership file and output
        output_directory = rf'{str(processed_directory)}\{key}'
        membership_file.to_csv(output_directory, sep='%', index=False)

    file_tools.delete_directory_contents(str(download_directory))
    print('Membership Files Created')
except:
    error = 'Error processing xls files'
    print(error)
    sys.exit()

try:
    if validation_tools.file_counter(path=processed_directory, extension='txt') == 0:
        raise Exception('Files not properly exported to correct location')
    if not validation_tools.file_name_clinic_validation(path=processed_directory, extension='txt',
                                                        active_clinics=active_clinic_keys,
                                                        delimiter='_', delimiter_index=0):
        missing_clinics = validation_tools.clinics_without_files(path=processed_directory, extension='txt',
                                                                 active_clinics=active_clinic_keys,
                                                                 delimiter='_', delimiter_index=0)
        unmapped_clinics = validation_tools.unmapped_clinic_files(path=processed_directory, extension='txt',
                                                                  active_clinics=active_clinic_keys,
                                                                  delimiter='_', delimiter_index=0)
        if missing_clinics['result'] & unmapped_clinics['result']:
            validation_error_message = missing_clinics['message'] + ' & ' + unmapped_clinics['message']
        elif missing_clinics['result']:
            validation_error_message = missing_clinics['message']
        else:
            validation_error_message = unmapped_clinics['message']
        raise Exception(validation_error_message)
    file_name_date_validation_result = validation_tools.file_name_date_validation(processed_directory, 'txt')
    # check to make sure that there is only 1 month worth of files in the directory
    if not file_name_date_validation_result['result']:
        raise Exception('File name not validated for correct month')
    # pickle success
    with open("./membership_process_success.pkl", 'wb') as file:
        pickle.dump(True, file)
except Exception as error:
    print(error)
finally:
    sys.exit()
