import os
import sys
import pandas as pd
import config
import pyodbc
from datetime import datetime as dt
from sqlalchemy.sql import text
import calendar
from pathlib import Path
import phc_tools
import validation_tools

pyodbc.pooling = False
######################################################################################################################


# check to see if there is a file to proceed (no error notification needed)
try:
    qip_file_count = validation_tools.file_counter(path=config.ereports_file_download_directory, extension='xls')
    if qip_file_count == 0:
        raise Exception
except:
    error = 'No QIP file to be uploaded'
    print(error)
    sys.exit()

# run measures upload for every iteration of qip file count
for qip_files in range(qip_file_count):
    # check if measures download was a success
    try:
        if not os.path.isfile("./measures_download_success.pkl"):
            raise Exception
    except:
        error = 'Measures Download or previous iteration of Measures Upload was not a success'
        print(error)
        sys.exit()

    try:
        # global variables
        download_directory = config.ereports_file_download_directory
        processed_directory = config.ereports_file_processed_directory

        # locate the earliest downloaded qip file in the unprocessed directory and if none exists, hard stop
        qip_file_path = phc_tools.get_first_file_path_modified(str(download_directory), walk=False,
                                                               files_extension='xls')

        # initialize logging dictionary
        log_dictionary = {'file_name': Path(qip_file_path).name, 'download_date': None, 'insert_count': None,
                          'warning_message': None, 'failure_step': None, 'is_success': False}
    except Exception as error:
        error = 'Error finding the earliest downloaded qip file in the unprocessed directory'
        print(error)
        sys.exit()

    try:

        # connect to partnership database to query active clinic values
        phc_db_engine = phc_tools.SQL_connection(keepass_filename=config.KDBX_FILE,
                                                       keepass_keyfile=config.KEY_PATH,
                                                       return_engine=True)

        phc_id_to_clinic_id_map = phc_tools.active_values_query(db_connection=phc_db_engine,
                                                                return_type='dict',
                                                                table_name='clinic_map',
                                                                query_col_1='CAST(phc_id AS VARCHAR(6)) AS phc_id',
                                                                query_col_2='id')

        measure_to_id_map = phc_tools.active_values_query(db_connection=phc_db_engine,
                                                          return_type='dict',
                                                          table_name='measures_map',
                                                          query_col_1='measure',
                                                          query_col_2='id')

        field_converters = {
            'QIP Result': lambda x: 1 if x == 'Numerator' else 0,
            'CIN': str,
            'Measure Name': lambda x: phc_tools.measure_converter(x, measure_to_id_map),
            'PCP': lambda x: phc_tools.clinic_converter(x, phc_id_to_clinic_id_map),
            'Member First Name': lambda x: x.replace("'", "''"),
            'Member Last Name': lambda x: x.replace("'", "''"),
            'Member Phone': phc_tools.format_phone_number,
            'Gender': lambda x: 'Female' if x == 'F' else 'Male',
            'NewMember': lambda x: 1 if x == 'Y' else 0,
            'DOB': pd.to_datetime
        }
    except:
        error = 'Failed querying active values from partnership database'
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    try:
        qip_df = pd.read_excel(io=qip_file_path, header=1, converters=field_converters)
        qip_df['Member First Name'] = qip_df['Member First Name'].fillna('')
        # mapping the raw / expected column names to the column names in the db
        qip_df.rename(
            columns={
                'CIN': 'cin',
                'PCP': 'clinic_id',
                'Member First Name': 'first_name',
                'Member Last Name': 'last_name',
                'DOB': 'dob',
                'Age': 'age',
                'Gender': 'sex',
                'NewMember': 'new_member',
                'Member Phone': 'phone_number',
                'Measure Name': 'measure_id',
                'QIP Result': 'is_compliant'
            }, inplace=True)

        # adding download_date, current year, and current month to df and log
        file_date = phc_tools.get_file_modified_date(qip_file_path, data_type_return=str)
        qip_df['download_date'] = file_date
        log_dictionary['download_date'] = file_date
        file_year = phc_tools.get_file_modified_year(qip_file_path, data_type_return=int)
        qip_df['year'] = file_year
        file_month = phc_tools.get_file_modified_month(qip_file_path, data_type_return=int)
        qip_df['month'] = file_month
    except Exception:
        error = 'Failed pushing and transforming file data into dataframe'
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    # validation block
    try:
        # assess that all clinics are mapped, if not, then hard stop
        unmapped_clinics = qip_df[~qip_df['clinic_id'].apply(lambda x: isinstance(x, int))][
            'clinic_id'].unique().tolist()
        if len(unmapped_clinics) != 0:
            unmapped_clinic_warning = 'Unmapped clinics: ' + ' & '.join(unmapped_clinics)
            raise Exception(unmapped_clinic_warning)
        qip_df = qip_df.astype({'clinic_id': int})
        # warning if there are active clinics missing from df
        df_missing_clinics = validation_tools.df_missing_values(dataframe=qip_df, column_name='clinic_id',
                                                                active_values=list(phc_id_to_clinic_id_map.values()))
        clinic_warning_message = 'Clinic - ' + df_missing_clinics['message']
        # see which measures are inactive or unmapped
        unmapped_measures = qip_df[~qip_df['measure_id'].apply(lambda x: isinstance(x, int))][
            'measure_id'].unique().tolist()
        if len(unmapped_measures) != 0:
            unmapped_measure_warning = 'Unmapped measures: ' + ' & '.join(unmapped_measures)
            print(unmapped_measure_warning)
        # filter down measures to only active measures in map
        qip_df = qip_df[qip_df['measure_id'].isin(measure_to_id_map.values())]
        qip_df = qip_df.astype({'measure_id': int})
        # warning if there are active measures missing from df
        df_missing_measures = validation_tools.df_missing_values(dataframe=qip_df, column_name='measure_id',
                                                                 active_values=list(measure_to_id_map.values()))
        measure_warning_message = 'Measure - ' + df_missing_measures['message']

        print('\n'.join([clinic_warning_message, measure_warning_message]))
        filtered_file_row_count = qip_df.shape[0]
        log_dictionary['insert_count'] = qip_df.shape[0]
        log_dictionary['warning_message'] = '\n'.join([clinic_warning_message, measure_warning_message])
    except Exception as error:
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    try:
        measure_temp_df = qip_df[['cin', 'clinic_id', 'measure_id', 'year', 'month', 'is_compliant', 'download_date']]
        create_temp_measures_history_table = """
        DROP TABLE IF EXISTS #measures_history_temp;
        CREATE TABLE #measures_history_temp (
          cin                CHAR(10)      NOT NULL REFERENCES partnership.dbo.membership (cin) ON DELETE CASCADE,
          clinic_id          INTEGER       NOT NULL REFERENCES partnership.dbo.clinic_map (id),
          measure_id         INTEGER       NOT NULL REFERENCES partnership.dbo.measures_map (id),
          year               INTEGER       NOT NULL,
          month              INTEGER       NOT NULL,
          is_compliant       BIT           NOT NULL,
          download_date      DATE          NOT NULL
        );
        """
        measure_temp_df.to_sql(con=phc_db_engine, name='#measures_history_temp', if_exists='append', index=False)

        temp_table_row_count_query = """
            SELECT
              COUNT(*)
            FROM #measures_history_temp;
            """
        temp_table_row_count = pd.read_sql(sql=temp_table_row_count_query, con=phc_db_engine).iloc[0, 0]
        if filtered_file_row_count != temp_table_row_count:
            raise Exception('File row count is not equal to temp table row count')
        print(
            f'Successfully filled out temp table with all {str(temp_table_row_count)} rows from dataframe for month {calendar.month_name[int(file_month)]}')
    except Exception as error:
        # error = 'Failed connecting to SQL Server and inserting rows into Measures Temp Table'
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    try:
        # check if there are qip members not yet in the membership table but in measures
        unmapped_records_query = """
            SELECT cin
            FROM #measures_history_temp
            EXCEPT
            SELECT cin
            FROM partnership.dbo.membership;
            """
        unmapped_records = pd.read_sql_query(con=phc_db_engine, sql=unmapped_records_query)

        # if there are qip members in membership but not yet in membership_history for the current month
        unenrolled_records_query = f"""
            SELECT cin
            FROM #measures_history_temp
            EXCEPT
            SELECT cin
            FROM partnership.dbo.membership_history WHERE year = {file_year} AND month = {file_month} ;
            """
        unenrolled_records = pd.read_sql_query(con=phc_db_engine, sql=unenrolled_records_query)

        if unmapped_records.shape[0] != 0 or unenrolled_records.shape[0] != 0:
            print(f'Need to create membership_temp table to fill out membership and membership_history')
            # if membership table has been updated for current month and year do the following

            query = "SELECT YEAR(MAX(download_date)) AS year, MONTH(MAX(download_date)) AS month FROM membership;"
            membership_last_updated = pd.read_sql_query(con=phc_db_engine, sql=query)

            same_month = dt.today().month == membership_last_updated['month'].iloc[0]
            same_year = dt.today().year == membership_last_updated['year'].iloc[0]
            if not (same_month and same_year):  # membership table not updated
                # skip inserting missing members into membership and membership_history
                raise ValueError(
                    'Membership table is not yet updated, will skip adding members, stop uploading measures')
            else:
                print('Membership table updated for the current month')
                # create membership_temp table to insert into membership
                membership_temp_df = qip_df[
                    ['cin', 'clinic_id', 'first_name', 'last_name', 'dob', 'sex', 'phone_number', 'new_member',
                     'download_date']]
                membership_temp_df = membership_temp_df.drop_duplicates()
                create_temp_membership_table = """
                DROP TABLE IF EXISTS #membership_temp;
                CREATE TABLE #membership_temp
                (
                  cin           CHAR(10) PRIMARY KEY,
                  clinic_id     INTEGER     NOT NULL REFERENCES clinic_map (id),
                  first_name    VARCHAR(50) NOT NULL DEFAULT '',
                  last_name     VARCHAR(50) NOT NULL,
                  dob           DATE        NOT NULL,
                  sex           VARCHAR(12),
                  phone_number  VARCHAR(12),
                  new_member    BIT,
                  download_date DATE
                );
                """
                with phc_db_engine.connect() as connection:
                    with connection.begin():
                        connection.execute(text(create_temp_membership_table))
                membership_temp_df.to_sql(con=phc_db_engine, name='#membership_temp', if_exists='append', index=False)
        else:
            print('No missing records in membership table and no unenrolled members from membership_history')

        if unmapped_records.shape[0] != 0:
            # insert unmapped records into membership and membership_history table
            unmapped_insert_into_membership = """
                        INSERT INTO membership (cin,
                                    clinic_id,
                                    first_name,
                                    last_name,
                                    dob,
                                    sex,
                                    phone_number,
                                    new_member,
                                    date_new_member,
                                    months_enrolled,
                                    months_missed,
                                    is_enrolled,
                                    is_qip_eligible,
                                    download_date)
            SELECT
              cin,
              clinic_id,
              COALESCE(first_name, '') AS first_name,
              last_name,
              dob,
              sex,
              phone_number,
              new_member,
              download_date AS date_new_member,
              1 AS months_enrolled,
              MONTH(GETDATE()) - 1 AS months_missed,
              1 AS is_enrolled,
              IIF(MONTH(GETDATE()) <= 3, 1, 0) AS is_qip_eligible,
              download_date
            FROM #membership_temp AS temp
            WHERE temp.cin NOT IN (
              SELECT cin
              FROM membership
            );
                 """

            with phc_db_engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(unmapped_insert_into_membership))
            print(f'Inserted {unmapped_records.shape[0]} rows into membership for new members')

            unmapped_insert_into_membership_history = """
            INSERT INTO membership_history (cin, clinic_id, year, month, is_qip_eligible, download_date)
            SELECT
              cin,
              clinic_id,
              YEAR(download_date),
              MONTH(download_date),
              is_qip_eligible,
              download_date
            FROM membership
            WHERE cin NOT IN (
              SELECT DISTINCT
                cin
              FROM membership_history
            );
                """

            with phc_db_engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(unmapped_insert_into_membership_history))
            print(f'Inserted {unmapped_records.shape[0]} rows into membership_history for new members')

        if unenrolled_records.shape[0] != 0:
            # update unmapped records in membership and insert into membership_history table
            unenrolled_update_membership = """
                UPDATE membership
                SET clinic_id = temp.clinic_id,
                    first_name = COALESCE(temp.first_name, ''),
                    last_name = temp.last_name,
                    dob = temp.dob,
                    sex = temp.sex,
                    phone_number = temp.phone_number,
                    new_member = temp.new_member,
                    months_enrolled = membership.months_enrolled + 1,
                    months_missed = MONTH(GETDATE()) - (membership.months_enrolled + 1),
                    is_enrolled = 1,
                    is_qip_eligible = IIF((MONTH(GETDATE()) - (membership.months_enrolled + 1)) <= 3, 1, 0),
                    download_date = temp.download_date
                FROM #membership_temp AS temp
                WHERE temp.cin = membership.cin AND membership.is_enrolled = 0;
                 """

            with phc_db_engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(unenrolled_update_membership))
            print(f'Updated {unenrolled_records.shape[0]} rows in membership for members not yet enrolled this month')

            unenrolled_insert_into_membership_history = f"""
            INSERT INTO membership_history (cin, clinic_id, year, month, is_qip_eligible, download_date)
            SELECT
              temp.cin,
              temp.clinic_id,
              YEAR(temp.download_date),
              MONTH(temp.download_date),
              membership.is_qip_eligible,
              temp.download_date
            FROM #membership_temp AS temp INNER JOIN membership ON temp.cin = membership.cin
            WHERE temp.cin NOT IN (
            SELECT cin
            FROM partnership.dbo.membership_history WHERE year = {file_year} AND month = {file_month}
            );
                """

            with phc_db_engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(unenrolled_insert_into_membership_history))
            print(
                f'Inserted {unenrolled_records.shape[0]} rows into membership_history for members not yet enrolled this month')

    except ValueError as error:
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()
    except:
        error = 'Failed checking for missing records and updating membership tables'
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    try:
        insert_measures_statement = """
        INSERT INTO measures_history (cin,
                                      clinic_id,
                                      measure_id,
                                      year,
                                      month,
                                      is_compliant,
                                      is_qip_eligible,
                                      download_date)
        SELECT
          measures_history.cin,
          measures_history.clinic_id,
          measures_history.measure_id,
          measures_history.year,
          measures_history.month,
          measures_history.is_compliant,
          membership.is_qip_eligible,
          measures_history.download_date
        FROM #measures_history_temp AS measures_history 
        INNER JOIN membership ON measures_history.cin = membership.cin;
            """
        with phc_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_measures_statement))
        print(f'Inserted {measure_temp_df.shape[0]} rows into measures_history')
    except:
        error = 'Failed to insert new data into measures_history table'
        print(error)
        log_dictionary['failure_step'] = str(error)
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()

    try:
        # move file to processed folder
        # get the year and month the file was downloaded to create a new output directory

        output_directory = rf'{str(processed_directory)}\{file_year}\{file_month:02d}'
        phc_tools.move_file(qip_file_path, to_dir=output_directory, prompt=False)

        renamed_xls_file = f'QIP_{file_month:02d}-{file_year}_{file_date}.xls'
        os.rename(os.path.join(output_directory, os.path.basename(qip_file_path)),
                  os.path.join(output_directory, renamed_xls_file))
        print(f'Moved QIP file to {output_directory} and renamed to {renamed_xls_file}')
        log_dictionary['is_success'] = True

        # assess how many more times measures upload script will need to run
        leftover_qip_file_count = validation_tools.file_counter(path=download_directory, extension='xls')
        if leftover_qip_file_count != 0:
            print(f'Need to rerun Measures Upload script {leftover_qip_file_count} more times')
        else:
            # remove pickle for measures download success
            print('No more QIP files needed to upload')
            if os.path.exists("./measures_download_success.pkl"):
                os.remove("./measures_download_success.pkl")
    except:
        error = 'Failed moving and renaming QIP file'
        print(error)
        log_dictionary['failure_step'] = str(error)
    finally:
        log = pd.DataFrame([log_dictionary])
        log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_measures_log', if_exists='append',
                   index=False)
        phc_db_engine.dispose()
        sys.exit()
