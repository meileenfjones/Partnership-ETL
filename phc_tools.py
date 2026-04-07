import os
import shutil
import pandas as pd
from datetime import datetime as dt
import re
from pykeepass import PyKeePass
from urllib.parse import quote_plus
from sqlalchemy import create_engine


########################################################################

def file_list(path, extension):
    """
    creates a list of file pathlib paths
    :param path: parent windows path where all files are located: pathlib Path
    :param extension: string extension of the files
    :return: list of file paths
    """
    return [file for file in path.glob(f'*.{extension}') if file.is_file()]


def measure_converter(x, mapper):
    """
    converts raw measure format into normalized names
    :param x: raw measure string with year at the end
    :param mapper: dictionary of input and output of our converter
    :return: if able to convert, mapper value; if not, raw value
    """
    # mapping the raw measure names to the measure names in the db
    if x.replace(r'\W+', '-')[0:-5] in mapper.keys():
        return mapper[x.replace(r'\W+', '-')[0:-5]]
    else:
        return x


def clinic_converter(x, mapper):
    """
    converts raw clinic format into normalized names
    :param x: raw clinic string
    :param mapper: dictionary of input and output of our converter
    :return: if not able convert, mapper value; if not, raw value
    """
    # mapping the raw clinic names to the clinic names in the db
    for key in mapper.keys():
        if key in x:
            return mapper[key]
    return x


def SQL_connection(keepass_filename, keepass_keyfile, return_engine=False):
    """
    creates a connection or engine to the SQL partnership database
    :param keepass_filename: keepass file path
    :param keepass_keyfile: keepass key file path
    :param return_engine: default false, boolean
    :return: if return_engine=True, returns engine; else, returns connection
    """
    # uses credentials to connect to PHP database
    keepass_db = PyKeePass(filename=keepass_filename, keyfile=keepass_keyfile)
    php_database = keepass_db.find_entries(title=SQL_entry, first=True)
    db_driver = 'driver=' + php_database.custom_properties['driver'] + ';'
    db_server = 'server=' + php_database.custom_properties['server'] + ';'
    db_name = 'database=' + php_database.custom_properties['database'] + ';'
    db_username = 'uid=' + php_database.username + ';'
    db_password = 'pwd=' + php_database.password + ';'
    db_connection_string = quote_plus(db_driver + db_server + db_name + db_username + db_password)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(db_connection_string), fast_executemany=True)
    if return_engine:
        return engine
    else:
        return engine.connect()


def active_values_query(db_connection, return_type, table_name, query_col_1, query_col_2=None):
    """
    intended to query active values from clinic_map or measure_map tables in SQL database
    :param db_connection: engine or connection to SQL database
    :param return_type: 'list' or 'dict'
    :param table_name: 'clinic_map' or 'measures_map'
    :param query_col_1: either the values of the list or the key of the dictionary
    :param query_col_2: default None, values of the dictionary is filled out
    :return: list of dictionary of clinics or measures
    """
    if return_type == 'list':
        query = f"""
        SELECT
          {query_col_1}
        FROM {table_name}
        WHERE is_active = 1;
        """
        return pd.read_sql(sql=query, con=db_connection).values.flatten().tolist()
    elif return_type == 'dict':
        query = f"""
        SELECT
          {query_col_1},
          {query_col_2}
        FROM {table_name}
        WHERE is_active = 1;
        """
        return dict(pd.read_sql(sql=query, con=db_connection).values)


def format_phone_number(phone_number):
    """
    :param phone_number: phone number column in the raw file
    :return: clean version of the phone number
    """
    # remove all special characters
    number = str(phone_number).replace(r'\W+', '')

    if len(number) == 10:
        # format to 123-123-1234
        return re.sub(r'(\d{3})(\d{3})(\d{4})', '\\1-\\2-\\3', number)
    elif len(number) == 7:
        # format to 123-1234
        return re.sub(r'(\d{3})(\d{4})', '\\1-\\2', number)
    else:
        return ''


def move_file(file_path_from_dir, to_dir, prompt=False):
    """
    :param file_path_from_dir:
    :param to_dir: name of the directory only
    :param prompt: to get input from the user or not if the file should be moved
    :return:
    """

    file_name = os.path.basename(file_path_from_dir)

    # prompt user to move otherwise not and continue
    if prompt is False:
        response = 'y'
    else:
        prompt_user = 'Move "{}" to "{}"? (y/n):'.format(file_path_from_dir, to_dir)

        while True:
            response = input(prompt_user)
            if response in ['y', 'n']:
                break

    # move file if desired
    if response == 'y':
        if os.path.exists(to_dir):
            shutil.move(file_path_from_dir, os.path.join(file_name, to_dir))
            print('Moved "{}" to "{}"'.format(file_path_from_dir, to_dir))
        else:
            os.makedirs(to_dir)
            shutil.move(file_path_from_dir, os.path.join(file_name, to_dir))
            print('Moved "{}" to "{}"'.format(file_path_from_dir, to_dir))


def get_file_modified_month(file_path, data_type_return):
    """
    :param file_path: path of the file
    :param data_type_return: data type to return: str or int
    :return: month that the file was last modified / downloaded
    """
    month_modified = dt.fromtimestamp(os.path.getmtime(file_path)).strftime('%m')
    if data_type_return == str:
        return month_modified
    elif data_type_return == int:
        return int(month_modified)


def get_file_modified_year(file_path, data_type_return):
    """
    :param file_path: path of the file
    :param data_type_return: data type to return: str or int
    :return: year that the file was last modified / downloaded
    """
    year_modified = dt.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y')
    if data_type_return == str:
        return year_modified
    elif data_type_return == int:
        return int(year_modified)


def get_file_modified_date(file_path, data_type_return):
    """
    :param file_path: path of the file
    :param data_type_return: data type to return: datetime or str
    :return: datetime modified of the file
    """
    date_modified = dt.fromtimestamp(os.path.getmtime(file_path))
    if data_type_return == dt:
        return pd.to_datetime(date_modified)
    elif data_type_return == str:
        return date_modified.strftime('%Y-%m-%d')


def get_last_file_path_modified(directory, walk, files_extension=None):
    """
    :param directory: directory to search for files
    :param walk: True if the directory should be walked through
    :param files_extension: if None all file extensions, otherwise specify extension files end with
    :return: the last file modified in the directory
    """
    file_paths_list = get_file_paths_list(directory, walk, files_extension)
    if len(file_paths_list) == 0:
        raise Exception('No file in {}'.format(directory))
    else:
        return max(file_paths_list, key=os.path.getmtime)


def get_first_file_path_modified(directory, walk, files_extension=None):
    """
    :param directory: directory to search for files
    :param walk: True if the directory should be walked through
    :param files_extension: if None all file extensions, otherwise specify extension files end with
    :return: the first file modified in the directory
    """
    file_paths_list = get_file_paths_list(directory, walk, files_extension)
    if len(file_paths_list) == 0:
        raise Exception('No file in {}'.format(directory))
    else:
        return min(file_paths_list, key=os.path.getmtime)


def get_file_paths_list(directory, walk, files_extension=None):
    """
    :param directory: directory to search for files
    :param walk: True if the directory should be walked through
    :param files_extension: if None all file extensions, otherwise specify extension files end with
    :return: the paths of the files in the directory
    """
    if walk is True:
        if files_extension is None:
            return [os.path.join(root, f) for root, _, files in os.walk(directory) for f in files]
        else:
            return [os.path.join(root, f) for root, _, files in os.walk(directory) for f in files if
                    f.endswith(files_extension)]
    else:
        if files_extension is None:
            return [os.path.join(directory, file) for file in os.listdir(directory)]
        else:
            return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(files_extension)]
