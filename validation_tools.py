from datetime import datetime


########################################################################

def file_counter(path, extension):
    """
    counts the number of files with a specific extension in a directory
    :param path: pathlib Path of directory where files are located
    :param extension: string extension of the files
    :return: integer count of files
    """
    file_count = [ext for ext in path.glob(f'*.{extension}') if ext.is_file()]
    return len(file_count)


def file_name_clinic_validation(path, extension, active_clinics, delimiter, delimiter_index):
    """
    validates if files are mapped to all active clinics and all active clinics are mapped to files
    :param path: pathlib Path of directory where files are located
    :param extension: string extension of the files
    :param active_clinics: list of active clinics which need to be matched inside file names
    :param delimiter: string of delimiter needed to split file names to get clinics
    :param delimiter_index: index of delimiter needed to split file names to get clinics
    :return: boolean; True if 1-1 match between files and active clinics
    """
    exported_files = [file for file in path.glob(f'*.{extension}') if file.is_file()]
    clinics_with_data = [file.name.split(delimiter)[delimiter_index] for file in exported_files]
    return set(map(str, active_clinics)) == set(clinics_with_data)


def clinics_without_files(path, extension, active_clinics, delimiter, delimiter_index):
    """
    gives result and message of whether there exists active clinics without files
    :param path: pathlib Path of directory where files are located
    :param extension: string extension of the files
    :param active_clinics: list of active clinics which need to be matched inside file names
    :param delimiter: string of delimiter needed to split file names to get clinics
    :param delimiter_index: index of delimiter needed to split file names to get clinics
    :return: dictionary of boolean result (if clinics without files exists) and corresponding message
    """
    exported_files = [file for file in path.glob(f'*.{extension}') if file.is_file()]
    clinics_with_data = [file.name.split(delimiter)[delimiter_index] for file in exported_files]
    missing_clinics = list(set(map(str, active_clinics)) - set(clinics_with_data))
    if len(missing_clinics) == 0:
        return dict({'result': False, 'message': 'All active clinics have data'})
    else:
        return dict({'result': True, 'message': 'Clinics without file data: ' + ' & '.join(missing_clinics)})


def unmapped_clinic_files(path, extension, active_clinics, delimiter, delimiter_index):
    """
    gives result and message of whether there exists files with unmapped clinics
    :param path: pathlib Path of directory where files are located
    :param extension: string extension of the files
    :param active_clinics: list of active clinics which need to be matched inside file names
    :param delimiter: string of delimiter needed to split file names to get clinics
    :param delimiter_index: index of delimiter needed to split file names to get clinics
    :return: dictionary of boolean result (if files with unmapped clinics exists) and corresponding message
    """
    exported_files = [file for file in path.glob(f'*.{extension}') if file.is_file()]
    clinics_with_data = [file.name.split(delimiter)[delimiter_index] for file in exported_files]
    unmapped_clinics = list(set(clinics_with_data) - set(map(str, active_clinics)))
    if len(unmapped_clinics) == 0:
        return dict({'result': False, 'message': 'No files are associated with unmapped clinics'})
    else:
        return dict({'result': True, 'message': 'Unmapped clinic files: ' + ' & '.join(unmapped_clinics)})


def df_value_validation(dataframe, column_name, active_values):
    """
    validates if values in a column of a dataframe are equal to all chosen active values
    :param dataframe: dataframe
    :param column_name: string of column name in dataframe
    :param active_values: list of active values which need to be matched to dataframe values
    :return: boolean; True if 1-1 match between df values and active values
    """
    df_values = list(dataframe[column_name].unique())
    return set(map(str, active_values)) == set(map(str, df_values))


def df_missing_values(dataframe, column_name, active_values):
    """
    gives result and message of whether there exists active values not found in dataframe
    :param dataframe: dataframe
    :param column_name: string of column name in dataframe
    :param active_values: list of active values which need to be matched to dataframe values
    :return: dictionary of boolean result (if active values are not in dataframe) and corresponding message
    """
    df_values = list(dataframe[column_name].unique())
    missing_values = list(set(map(str, active_values)) - set(map(str, df_values)))
    if len(missing_values) == 0:
        return dict({'result': False, 'message': 'No missing values'})
    else:
        return dict({'result': True, 'message': 'Missing df values: ' + ' & '.join(missing_values)})


def df_unmapped_values(dataframe, column_name, active_values):
    """
    gives result and message of whether there exists unmapped values in dataframe not found in active values
    :param dataframe: dataframe
    :param column_name: string of column name in dataframe
    :param active_values: list of active values which need to be matched to dataframe values
    :return: dictionary of boolean result (if there are unmapped values in dataframe) and corresponding message
    """
    df_values = list(dataframe[column_name].unique())
    unmapped_values = list(set(map(str, df_values)) - set(map(str, active_values)))
    if len(unmapped_values) == 0:
        return dict({'result': False, 'message': 'All values mapped'})
    else:
        return dict({'result': True, 'message': 'Unmapped df values: ' + ' & '.join(unmapped_values)})


def file_name_date_validation(path, extension):
    """
    gives dictionary about whether the file name contains the right month and file_year and file_month
    :param path: pathlib Path of directory where files are located
    :param extension: string extension of the files
    :return: dictionary of boolean result (file names are validated for date) and file_year and file_month if True
    """
    file_months = []
    exported_files = [file for file in path.glob(f'*.{extension}') if file.is_file()]
    for file in exported_files:
        file_date_string = (file.name.split('_')[-1]).split('.')[0]
        file_year = file_date_string.split('-')[1]
        file_month = file_date_string.split('-')[0]
        file_date = datetime.strptime(file_date_string, '%m-%Y')
        if file_date not in file_months:
            file_months.append(file_date)
    if len(file_months) == 0:
        print('Data for no months found in file location')
        return dict({'result': False})
    elif len(file_months) > 1:
        print('Data for multiple months found in file location')
        return dict({'result': False})
    elif len(file_months) == 1 and file_date != datetime.today().replace(day=1, hour=0, minute=0, second=0,
                                                                         microsecond=0):
        print('1 month of data but not current month')
        return dict({'result': True, 'file_year': file_year, 'file_month': file_month})
    else:
        print('Data from current month only')
        return dict({'result': True, 'file_year': file_year, 'file_month': file_month})
