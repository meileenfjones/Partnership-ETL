import config
import os
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import sys
from sqlalchemy.exc import DataError
import shutil
import calendar
import validation_tools
import phc_tools
#################################################################

try:
    if not os.path.isfile("./membership_process_success.pkl"):
        raise Exception
except Exception:
    error = 'Membership Process was not a success'
    print(error)
    sys.exit()

try:
    # connect to partnership database to query active clinic values
    phc_db_engine = phc_tools.SQL_connection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                                   return_engine=True)

    clinic_key_to_id_map = phc_tools.active_values_query(db_connection=phc_db_engine,
                                                         return_type='dict',
                                                         table_name='clinic_map',
                                                         query_col_1='clinic_key',
                                                         query_col_2='id')

except:
    error = 'Failed querying active values from partnership database'
    print(error)
    phc_db_engine.dispose()
    sys.exit()

# pull all file data into a dataframe and add/rename columns
try:
    processed_directory = config.membership_files_processed_directory
    file_name_date_validation_result = validation_tools.file_name_date_validation(processed_directory, 'txt')
    file_year = file_name_date_validation_result['file_year']
    file_month = file_name_date_validation_result['file_month']
    processed_directory.joinpath('PHP-Processed-To-Server').joinpath(file_year).mkdir(exist_ok=True)
    # make month folder and save path for moving files later
    monthly_processed_to_server_path = processed_directory.joinpath('PHP-Processed-To-Server').joinpath(
        file_year).joinpath(file_month)
    monthly_processed_to_server_path.mkdir(exist_ok=True)

    # initialize logging dictionary
    log_dictionary = {
        'year': None,
        'month': None,
        'update_count': None,
        'insert_count': None,
        'enrolled_count': None,
        'qip_eligible_count': None,
        'failure_step': None,
        'is_success': False
    }

    # unprocessed PHP txt files path
    exported_txt_files = phc_tools.file_list(path=processed_directory, extension='txt')

    uploading_df = pd.DataFrame()

    for file in exported_txt_files:
        data = pd.read_csv(file, sep='%', usecols=['BIC_HIK', 'Clinic', 'FirstName', 'LastName', 'DOB', 'Sex',
                                                   'PhoneNumber', 'Address', 'City', 'State', 'ZipCode',
                                                   'RecordNumber', 'RP', 'EffDate', 'EndDate', 'OtherInsurance',
                                                   'NewMember', 'PHPCurrentFlag', 'LastUpdated'],
                           converters={'Clinic': lambda x: phc_tools.clinic_converter(x, clinic_key_to_id_map)})
        uploading_df = pd.concat([uploading_df, data], ignore_index=True, axis=0)
        print('Uploaded ' + str(data.shape[0]) + ' records from ' + file.name)

    monthly_files_row_count = uploading_df.shape[0]
    # renaming columns to match DDL
    uploading_df.rename(
        columns={'BIC_HIK': 'cin',
                 'Clinic': 'clinic_id',
                 'FirstName': 'first_name',
                 'LastName': 'last_name',
                 'DOB': 'dob',
                 'Sex': 'sex',
                 'PhoneNumber': 'phone_number',
                 'Address': 'address',
                 'City': 'city',
                 'State': 'state',
                 'ZipCode': 'zip_code',
                 'RecordNumber': 'record_number',
                 'RP': 'rp',
                 'EffDate': 'eff_date',
                 'EndDate': 'end_date',
                 'OtherInsurance': 'other_insurance',
                 'NewMember': 'new_member',
                 'PHPCurrentFlag': 'is_enrolled',
                 'LastUpdated': 'download_date'},
        inplace=True)

    # if first_name is null then fill it with an empty string
    uploading_df['first_name'] = uploading_df['first_name'].fillna('')
except Exception:
    error = 'Failed pushing and transforming file data into dataframe'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

# validation block
try:
    # verify all clinic values are present
    if not validation_tools.df_value_validation(dataframe=uploading_df, column_name='clinic_id',
                                                active_values=list(clinic_key_to_id_map.values())):
        missing_clinics = validation_tools.df_missing_values(dataframe=uploading_df, column_name='clinic_id',
                                                             active_values=list(clinic_key_to_id_map.values()))
        unmapped_clinics = validation_tools.df_unmapped_values(dataframe=uploading_df, column_name='clinic_id',
                                                               active_values=list(clinic_key_to_id_map.values()))
        if missing_clinics['result'] & unmapped_clinics['result']:
            validation_error_message = missing_clinics['message'] + ' & ' + unmapped_clinics['message']
        elif missing_clinics['result']:
            validation_error_message = missing_clinics['message']
        else:
            validation_error_message = unmapped_clinics['message']
        raise Exception(validation_error_message)

    # if there are null values in columns that are required in DDL, hard stop
    if uploading_df[['cin', 'clinic_id', 'first_name', 'last_name', 'dob', 'new_member', 'download_date',
                     'is_enrolled']].isnull().values.any():
        raise Exception('A required field is null and cannot be inputted into DDL')

    # if there is a CIN that is not exactly 10 characters long, hard stop and print where error occurs
    if uploading_df['cin'].str.len().max() != uploading_df['cin'].str.len().min() or uploading_df[
        'cin'].str.len().max() != 10 or uploading_df['cin'].str.contains('\\.0').any():
        cin_error_map = dict(
            pd.merge(uploading_df[((uploading_df['cin'].str.len() != 10) | (uploading_df['cin'].str.contains('\\.0')))],
                     pd.DataFrame(clinic_key_to_id_map.items(), columns=['clinic', 'clinic_id']),
                     on='clinic_id')[['cin', 'clinic']].values)
        cin_error_message = 'CIN not 10 characters long: ' + ' & '.join(
            [key + ' in ' + str(value) + ' file' for key, value in cin_error_map.items()])
        raise Exception(cin_error_message)
    print('Dataframe validation complete')
except Exception as error:
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

# create temp table DDL and push dataframe inside
try:
    create_temp_table = f"""
    DROP TABLE IF EXISTS #temp_membership;
    CREATE TABLE #temp_membership
    (
      cin             CHAR(10) PRIMARY KEY,
      clinic_id       INTEGER     NOT NULL REFERENCES clinic_map (id),
      first_name      VARCHAR(50) NOT NULL,
      last_name       VARCHAR(50) NOT NULL,
      dob             DATE        NOT NULL,
      sex             VARCHAR(12),
      phone_number    VARCHAR(12),
      address         VARCHAR(500),
      city            VARCHAR(500),
      state           VARCHAR(20),
      zip_code        VARCHAR(10),
      record_number   VARCHAR(20),
      rp              VARCHAR(4),
      eff_date        DATE,
      end_date        DATE,
      other_insurance VARCHAR(4),
      new_member      BIT         NOT NULL,
      is_enrolled     BIT         NOT NULL,
      download_date   DATE        NOT NULL
    );
            """
    with phc_db_engine.connect() as connection:
        with connection.begin():
            connection.execute(text(create_temp_table))
    uploading_df.to_sql(con=phc_db_engine, name='#temp_membership', if_exists='append', index=False)
    temp_table_row_count_query = """
    SELECT
      COUNT(*)
    FROM #temp_membership;
    """
    temp_table_row_count = pd.read_sql(sql=temp_table_row_count_query, con=phc_db_engine).iloc[0, 0]
    if monthly_files_row_count != temp_table_row_count:
        raise Exception('File row count is not equal to temp table row count')
    print(
        f'Successfully filled out temp table with all {str(temp_table_row_count)} rows from dataframe for month {calendar.month_name[int(file_month)]}')
except DataError:
    error = 'Unaccounted error inputting dataframe into temp table because does not fit DDL'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()
except Exception as error:
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

# check to see if we should reset columns and reset columns based on month of data
try:
    # get file download month and compare it to the month last updated in membership
    download_date_query = """
    SELECT
      MAX(download_date)
    FROM #temp_membership;
    """
    download_date = pd.read_sql(sql=download_date_query, con=phc_db_engine).iloc[0, 0]
    download_month = int(download_date.strftime("%m"))
    last_update_query = """
    SELECT
      MAX(download_date)
    FROM partnership.dbo.membership;
    """
    last_updated_date = pd.read_sql(sql=last_update_query, con=phc_db_engine).iloc[0, 0]
    last_updated_month = int(last_updated_date.strftime("%m"))
    # if calendar year was completed, then include month_enrolled in reset columns
    if last_updated_month == 12 and download_month == 1:
        print('Calendar year completed: Need to reset for next year')
        reset_columns_command = """
        UPDATE partnership.dbo.membership
        SET is_enrolled = 0,
            is_qip_eligible = 0,
            months_enrolled = 0
        FROM partnership.dbo.membership;
        """
        reset_columns = 'is_enrolled, is_qip_eligible, and months_enrolled'
    elif download_month == last_updated_month:
        raise Exception('Already updated this month')
    elif download_month != last_updated_month + 1:
        raise Exception('Potentially missed a month')
    else:  # download_month == last_updated_month + 1:
        print('Correct month to update')
        reset_columns_command = """
        UPDATE partnership.dbo.membership
        SET is_enrolled = 0,
            is_qip_eligible = 0
        FROM partnership.dbo.membership;
        """
        reset_columns = 'is_enrolled and is_qip_eligible'
    # reset membership columns
    transactional_session = Session(phc_db_engine)
    result = transactional_session.execute(text(reset_columns_command))
    answer = None
    while answer not in ['y', 'n']:
        answer = input(f'Reset {reset_columns} columns to 0 in membership, commit? (y/n): ')
        if answer == 'y':
            transactional_session.commit()
        elif answer == 'n':
            transactional_session.rollback()
            raise Exception('Rollback on resetting membership columns')
        else:
            print('Please input "y" or "n" before proceeding')
except Exception as error:
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    update_count_query = """
    SELECT
      COUNT(*) AS update_count
    FROM #temp_membership AS temp
    WHERE temp.cin IN (
      SELECT
        cin
      FROM partnership.dbo.membership
    );
    """
    update_count = pd.read_sql(sql=update_count_query, con=phc_db_engine).iloc[0, 0]

    update_membership = """
    UPDATE partnership.dbo.membership
    SET cin               = temp.cin,
        clinic_id         = temp.clinic_id,
        first_name        = temp.first_name,
        last_name         = temp.last_name,
        dob               = temp.dob,
        sex               = temp.sex,
        phone_number      = temp.phone_number,
        address           = temp.address,
        city              = temp.city,
        state             = temp.state,
        zip_code          = temp.zip_code,
        record_number     = temp.record_number,
        rp                = temp.rp,
        eff_date          = temp.eff_date,
        end_date          = temp.end_date,
        other_insurance   = temp.other_insurance,
        new_member        = temp.new_member,
        date_new_member   = IIF(temp.new_member = 1, temp.download_date, partnership.dbo.membership.date_new_member),
        months_enrolled   = partnership.dbo.membership.months_enrolled + 1, -- if new member set months enrolled to 1, if appeared this month add 1
        is_enrolled       = temp.is_enrolled,
        download_date     = temp.download_date,
        updated_at        = GETDATE()
    FROM #temp_membership AS temp
    WHERE partnership.dbo.membership.cin = temp.cin AND
          temp.cin IN (
            SELECT cin
            FROM partnership.dbo.membership
          );
    """

    transactional_session = Session(phc_db_engine)
    result = transactional_session.execute(text(update_membership))
    answer = None
    while answer not in ['y', 'n']:
        answer = input(f'Update membership with {update_count} rows, commit? (y/n): ')
        if answer == 'y':
            transactional_session.commit()
            log_dictionary['update_count'] = update_count
        elif answer == 'n':
            transactional_session.rollback()
            raise Exception('Rollback on updating membership')
        else:
            print('Please input "y" or "n" before proceeding')
except:
    error = 'Error update membership'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    NR_query = """
    SELECT
      temp.clinic_id,
      temp.cin
    FROM #temp_membership AS temp
    WHERE temp.cin NOT IN (
      SELECT
        cin
      FROM partnership.dbo.membership
    )
    ORDER BY clinic_id;
      """

    NR_df = pd.read_sql(sql=NR_query, con=phc_db_engine)
    NR_file_name = f'NR_{file_month}-{file_year}.txt'  # NR_04-2024.txt
    NR_df.to_csv(monthly_processed_to_server_path.joinpath(NR_file_name), sep=',', index=False)
except:
    error = 'NR file creation failed'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    insert_count_query = """
    SELECT
      COUNT(*) AS insert_count
    FROM #temp_membership AS temp
    WHERE temp.cin NOT IN (
      SELECT
        cin
      FROM partnership.dbo.membership
    );
    """
    insert_count = pd.read_sql(sql=insert_count_query, con=phc_db_engine).iloc[0, 0]

    insert_membership = """
    INSERT INTO partnership.dbo.membership (cin, clinic_id, first_name, last_name, dob, sex, phone_number, address,
                                            city, state, zip_code, record_number, rp, eff_date, end_date, other_insurance,
                                            new_member, date_new_member, months_enrolled, is_enrolled, download_date)
    SELECT
      temp.cin,
      temp.clinic_id,
      temp.first_name,
      temp.last_name,
      temp.dob,
      temp.sex,
      temp.phone_number,
      temp.address,
      temp.city,
      temp.state,
      temp.zip_code,
      temp.record_number,
      temp.rp,
      temp.eff_date,
      temp.end_date,
      temp.other_insurance,
      temp.new_member,
      temp.download_date AS date_new_member, -- if new member then fill out date_new_member else not
      1 AS months_enrolled,                  -- if new member set months enrolled to 1
      temp.is_enrolled,
      temp.download_date
    FROM #temp_membership AS temp
    WHERE temp.cin NOT IN (
      SELECT
        cin
      FROM partnership.dbo.membership
    );
    """

    transactional_session = Session(phc_db_engine)
    result = transactional_session.execute(text(insert_membership))
    answer = None
    while answer not in ['y', 'n']:
        answer = input(f'Insert {insert_count} rows into membership, commit? (y/n): ')
        if answer == 'y':
            transactional_session.commit()
            log_dictionary['insert_count'] = insert_count
        elif answer == 'n':
            transactional_session.rollback()
            raise Exception('Rollback on inserting membership')
        else:
            print('Please input "y" or "n" before proceeding')
except:
    error = 'Error inserting into membership'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    # if member is not enrolled in December, then automatically not QIP eligible
    update_months_missed_and_qip_eligible = f"""
    UPDATE partnership.dbo.membership
    SET months_missed   = {int(file_month)} - membership.months_enrolled,
        is_qip_eligible = CASE
                            WHEN {int(file_month)} IN (1, 2, 3)
                              THEN 1
                            WHEN {int(file_month)} = 12 AND membership.is_enrolled = 0
                              THEN 0
                            WHEN {int(file_month)} - membership.months_enrolled > 3
                              THEN 0
                            WHEN {int(file_month)} - membership.months_enrolled <= 3
                              THEN 1
          END;
    """

    transactional_session = Session(phc_db_engine)
    result = transactional_session.execute(text(update_months_missed_and_qip_eligible))
    answer = None
    while answer not in ['y', 'n']:
        answer = input(f'Update months_missed and is_qip_eligible columns for all rows, commit? (y/n): ')
        if answer == 'y':
            transactional_session.commit()
        elif answer == 'n':
            transactional_session.rollback()
            raise Exception('Rollback on updating months missed and is qip eligible column')
        else:
            print('Please input "y" or "n" before proceeding')
    print('Completed updating and inserting into membership table')
except Exception as e:
    error = 'Error updating months missed and is qip eligible column: ' + str(e)
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    insert_membership_history = """
    INSERT INTO partnership.dbo.membership_history (cin, clinic_id, year, month, is_qip_eligible, download_date)
    SELECT
      cin,
      clinic_id,
      YEAR(download_date) AS year,
      MONTH(download_date) AS month,
      is_qip_eligible,
      download_date
    FROM partnership.dbo.membership
    WHERE is_enrolled = 1;
    """
    transactional_session = Session(phc_db_engine)
    result = transactional_session.execute(text(insert_membership_history))
    answer = None
    while answer not in ['y', 'n']:
        answer = input(
            f"Insert {calendar.month_name[int(file_month)]}'s {str(monthly_files_row_count)} members into membership_history, commit? (y/n): ")
        if answer == 'y':
            transactional_session.commit()
        elif answer == 'n':
            transactional_session.rollback()
            raise Exception(
                f"Rollback on inserting {calendar.month_name[int(file_month)]}'s {str(monthly_files_row_count)} members into membership_history")
        else:
            print('Please input "y" or "n" before proceeding')
    print('Completed inserting into membership_history table')
except:
    error = 'Error insert this months php membership patients into membership_history'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    enrolled_and_qip_eligible_count_query = """
    SELECT
      SUM(IIF(is_enrolled = 1, 1, 0)) AS enrolled_count,
      SUM(IIF(is_qip_eligible = 1, 1, 0)) AS qip_eligible_count
    FROM partnership.dbo.membership;
    """
    enrolled_and_qip_eligible_count = pd.read_sql(sql=enrolled_and_qip_eligible_count_query, con=phc_db_engine)
    enrolled_count = enrolled_and_qip_eligible_count.iloc[0, 0]
    qip_eligible_count = enrolled_and_qip_eligible_count.iloc[0, 1]
    log_dictionary['enrolled_count'] = enrolled_count
    print(f"{calendar.month_name[int(file_month)]}'s total enrolled_count: {str(enrolled_count)}")
    log_dictionary['qip_eligible_count'] = qip_eligible_count
    print(f"{calendar.month_name[int(file_month)]}'s total qip_eligible_count: {str(qip_eligible_count)}")
except:
    error = 'Error logging enrolled_count & qip_eligible_count'
    print(error)
    log_dictionary['failure_step'] = str(error)
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()

try:
    # Moved files to processed folder
    for file in exported_txt_files:
        shutil.move(file, monthly_processed_to_server_path.joinpath(file.name))
    print('SUCCESS')
    log_dictionary['is_success'] = True
    # reset pickles
    os.remove("./membership_download_success.pkl")
    os.remove("./membership_process_success.pkl")
except:
    error = 'Error moving files into processed folder'
    print(error)
    log_dictionary['failure_step'] = str(error)
finally:
    log = pd.DataFrame([log_dictionary])
    log.to_sql(con=phc_db_engine, schema='partnership.dbo', name='etl_membership_log', if_exists='append', index=False)
    phc_db_engine.dispose()
    sys.exit()
