import sys
import config
import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy.sql import text
import phc_tools
#################################################################
try:
    # connect to partnership database to query active clinic values
    phc_db_engine = phc_tools.SQL_connection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH,
                                                   return_engine=True)
except:
    error = 'Failed connection to partnership database'
    print(error)
    phc_db_engine.dispose()
    sys.exit()

try:
    # update established epic_pat_id matches due to merges if needed
    merged_epic_pat_ids_query = """
    SELECT
      pat_merge_history.pat_id AS epic_pat_id,
      membership.cin
    FROM partnership.dbo.membership
      INNER JOIN Clarity_SA000.dbo.pat_merge_history
                 ON membership.epic_pat_id = pat_merge_history.patient_mrg_hist
    WHERE membership.epic_pat_id != pat_merge_history.pat_id;"""

    merged_epic_pat_ids = pd.read_sql(sql=merged_epic_pat_ids_query, con=phc_db_engine)

    if merged_epic_pat_ids.shape[0] > 0:
        update_merged_epic_pat_id_statement = """
            UPDATE partnership.dbo.membership
            SET epic_pat_id = pat_merge_history.pat_id
            FROM Clarity_SA000.dbo.pat_merge_history
            WHERE membership.epic_pat_id = pat_merge_history.patient_mrg_hist;
            """
        # update Membership with merged epic_pat_id
        with phc_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_merged_epic_pat_id_statement))

        # log the updates to apps_log.membership_matching_log
        merged_epic_pat_ids.to_sql(con=phc_db_engine, schema='partnership.dbo', name='membership_matching_log',
                                   if_exists='append', index=False)
        print('Successfully updated Membership table with merged epic_pat_ids')
    else:
        print('No merged epic_pat_ids updates needed')
except:
    error = 'Failed to update merged epic_pat_id in Membership table'
    print(error)
    phc_db_engine.dispose()
    sys.exit()

try:
    # unmapped partnership members and EPIC patients linked together by date of birth
    initial_match_query = """
    WITH phc_members AS (
      SELECT
        cin,
        dob AS phc_dob,
        REPLACE(TRIM(LOWER(first_name)), '-', ' ') AS phc_first_name,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', first_name + ' ') > 0
                       THEN SUBSTRING(first_name, 1, CHARINDEX(' ', first_name + ' ') - 1)
                     ELSE first_name
          END)) AS first_name_1,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', first_name + ' ') > 0
                       AND CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) >
                           CHARINDEX(' ', first_name + ' ')
                       THEN SUBSTRING(
                       first_name,
                       CHARINDEX(' ', first_name + ' ') + 1,
                       CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1)
                         - CHARINDEX(' ', first_name + ' ') - 1
                            )
          END)) AS first_name_2,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', first_name + ' ') > 0
                       AND CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) >
                           CHARINDEX(' ', first_name + ' ')
                       AND CHARINDEX(' ', first_name + ' ',
                                     CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) + 1)
                            > CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1)
                       THEN SUBSTRING(
                       first_name,
                       CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) + 1,
                       CHARINDEX(' ', first_name + ' ',
                                 CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) + 1)
                         - CHARINDEX(' ', first_name + ' ', CHARINDEX(' ', first_name + ' ') + 1) - 1
                            )
          END)) AS first_name_3,
        REPLACE(TRIM(LOWER(last_name)), '-', ' ') AS phc_last_name,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', last_name + ' ') > 0
                       THEN SUBSTRING(last_name, 1, CHARINDEX(' ', last_name + ' ') - 1)
                     ELSE last_name
          END)) AS last_name_1,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', last_name + ' ') > 0
                       AND CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) >
                           CHARINDEX(' ', last_name + ' ')
                       THEN SUBSTRING(
                       last_name,
                       CHARINDEX(' ', last_name + ' ') + 1,
                       CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1)
                         - CHARINDEX(' ', last_name + ' ') - 1
                            )
          END)) AS last_name_2,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', last_name + ' ') > 0
                       AND CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) >
                           CHARINDEX(' ', last_name + ' ')
                       AND CHARINDEX(' ', last_name + ' ',
                                     CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) + 1)
                            > CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1)
                       THEN SUBSTRING(
                       last_name,
                       CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) + 1,
                       CHARINDEX(' ', last_name + ' ',
                                 CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) + 1)
                         - CHARINDEX(' ', last_name + ' ', CHARINDEX(' ', last_name + ' ') + 1) - 1
                            )
          END)) AS last_name_3
      FROM partnership.dbo.membership
      WHERE membership.epic_pat_id IS NULL
    ),
         epic_pts AS (
      SELECT
        patient.pat_id AS epic_pat_id,
        CAST(birth_date AS date) AS epic_dob,
        TRIM(LOWER(pat_first_name)) AS epic_first_name,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_first_name + ' ') > 0
                       THEN SUBSTRING(pat_first_name, 1, CHARINDEX(' ', pat_first_name + ' ') - 1)
                     ELSE pat_first_name
          END)) AS first_name_1,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_first_name + ' ') > 0
                       AND CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) >
                           CHARINDEX(' ', pat_first_name + ' ')
                       THEN SUBSTRING(
                       pat_first_name,
                       CHARINDEX(' ', pat_first_name + ' ') + 1,
                       CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1)
                         - CHARINDEX(' ', pat_first_name + ' ') - 1
                            )
          END)) AS first_name_2,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_first_name + ' ') > 0
                       AND CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) >
                           CHARINDEX(' ', pat_first_name + ' ')
                       AND CHARINDEX(' ', pat_first_name + ' ',
                                     CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) + 1)
                            > CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1)
                       THEN SUBSTRING(
                       pat_first_name,
                       CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) + 1,
                       CHARINDEX(' ', pat_first_name + ' ',
                                 CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) + 1)
                         - CHARINDEX(' ', pat_first_name + ' ', CHARINDEX(' ', pat_first_name + ' ') + 1) - 1
                            )
          END)) AS first_name_3,
        TRIM(LOWER(pat_last_name)) AS epic_last_name,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_last_name + ' ') > 0
                       THEN SUBSTRING(pat_last_name, 1, CHARINDEX(' ', pat_last_name + ' ') - 1)
                     ELSE pat_last_name
          END)) AS last_name_1,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_last_name + ' ') > 0
                       AND CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) >
                           CHARINDEX(' ', pat_last_name + ' ')
                       THEN SUBSTRING(
                       pat_last_name,
                       CHARINDEX(' ', pat_last_name + ' ') + 1,
                       CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1)
                         - CHARINDEX(' ', pat_last_name + ' ') - 1
                            )
          END)) AS last_name_2,
        TRIM(LOWER(CASE
                     WHEN CHARINDEX(' ', pat_last_name + ' ') > 0
                       AND CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) >
                           CHARINDEX(' ', pat_last_name + ' ')
                       AND CHARINDEX(' ', pat_last_name + ' ',
                                     CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) + 1)
                            > CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1)
                       THEN SUBSTRING(
                       pat_last_name,
                       CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) + 1,
                       CHARINDEX(' ', pat_last_name + ' ',
                                 CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) + 1)
                         - CHARINDEX(' ', pat_last_name + ' ', CHARINDEX(' ', pat_last_name + ' ') + 1) - 1
                            )
          END)) AS last_name_3,
        ROW_NUMBER()
          OVER (PARTITION BY pat_name,birth_date ORDER BY update_date DESC,rec_create_date DESC) AS row_priority
      FROM Clarity_SA000.dbo.patient
      WHERE patient.pat_id NOT IN (
        SELECT epic_pat_id FROM partnership.dbo.membership WHERE epic_pat_id IS NOT NULL
      )
    )
    SELECT
      epic_pat_id,
      cin,
      phc_dob AS dob,
      CONCAT(phc_first_name, ' ', phc_last_name) AS phc_name,
      CONCAT(phc_last_name, ' ', phc_first_name) AS phc_name_flipped,
      CONCAT(epic_first_name, ' ', epic_last_name) AS epic_name,
      CASE
        WHEN (
               epic_pts.first_name_1 = phc_members.first_name_1 OR
               epic_pts.first_name_2 = phc_members.first_name_1 OR
               epic_pts.first_name_3 = phc_members.first_name_1 OR
               epic_pts.first_name_1 = phc_members.first_name_2 OR
               epic_pts.first_name_2 = phc_members.first_name_2 OR
               epic_pts.first_name_3 = phc_members.first_name_2 OR
               epic_pts.first_name_1 = phc_members.first_name_3 OR
               epic_pts.first_name_2 = phc_members.first_name_3 OR
               epic_pts.first_name_3 = phc_members.first_name_3
               ) AND
             (
               epic_pts.last_name_1 = phc_members.last_name_1 OR
               epic_pts.last_name_2 = phc_members.last_name_1 OR
               epic_pts.last_name_3 = phc_members.last_name_1 OR
               epic_pts.last_name_1 = phc_members.last_name_2 OR
               epic_pts.last_name_2 = phc_members.last_name_2 OR
               epic_pts.last_name_3 = phc_members.last_name_2 OR
               epic_pts.last_name_1 = phc_members.last_name_3 OR
               epic_pts.last_name_2 = phc_members.last_name_3 OR
               epic_pts.last_name_3 = phc_members.last_name_3
               ) THEN 1
        ELSE 0 END AS name_match
    FROM phc_members
      -- always match on date of birth
      INNER JOIN epic_pts ON phc_members.phc_dob = epic_pts.epic_dob
    WHERE row_priority = 1 AND
          -- matches on the first letter of first or last name
          (LEFT(phc_members.phc_first_name, 1) = LEFT(epic_pts.epic_first_name, 1) OR
           LEFT(phc_members.phc_last_name, 1) = LEFT(epic_pts.epic_last_name, 1) OR
           LEFT(phc_members.phc_first_name, 1) = LEFT(epic_pts.epic_last_name, 1) OR
           LEFT(phc_members.phc_last_name, 1) = LEFT(epic_pts.epic_first_name, 1));
      """

    matches = pd.read_sql(sql=initial_match_query, con=phc_db_engine)
except:
    error = 'Failed querying potential matches between PHC members and EPIC patients'
    print(error)
    phc_db_engine.dispose()
    sys.exit()

try:
    # creates similarity score between the name of the PHC member and the EPIC patient
    # due to possibility of flipped first and last name, will also check similarity of flipped name and pick highest
    matches['fuzzy_score'] = matches.apply(
        lambda x: max(fuzz.ratio(x.phc_name, x.epic_name), fuzz.ratio(x.phc_name_flipped, x.epic_name)), axis=1)
    # chooses only rows with a similarity score greater than 85% and will only select 1 row per cin and epic_pat_id score
    best_matches = matches[(matches['fuzzy_score'] > 85) | (matches['name_match'] == 1)].sort_values(
        by=['cin', 'fuzzy_score'], ascending=[True, False], na_position='last').groupby(['cin']).nth(0)

    print('Created best matches between PHC members and EPIC patients')
except:
    error = 'Failed using rapidfuzz to find best matches for unmapped Partnership members'
    print(error)
    phc_db_engine.dispose()
    sys.exit()

try:
    # if there are matches, update epic_pat_id
    if best_matches.shape[0] > 0:
        best_matches.to_sql(con=phc_db_engine, name='#temp_membership_patient_match', if_exists='replace', index=False)
        temp_membership_patient_match_count_query = """SELECT COUNT(*) FROM #temp_membership_patient_match;"""
        temp_table_row_count = pd.read_sql(sql=temp_membership_patient_match_count_query, con=phc_db_engine).iloc[0, 0]

        update_epic_pat_id = """
            UPDATE partnership.dbo.membership
            SET epic_pat_id = temp.epic_pat_id
            FROM #temp_membership_patient_match AS temp
            WHERE temp.cin = membership.cin;
            """

        # update Membership with new epic_pat_id matches
        with phc_db_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_epic_pat_id))
        print(f'Successfully updated Membership table with {temp_table_row_count} matches')

        # log the updates to apps_log.php_matching_log
        best_matches[['epic_pat_id', 'cin', 'dob', 'phc_name', 'epic_name', 'fuzzy_score']].to_sql(con=phc_db_engine,
                                                                                                   schema='partnership.dbo',
                                                                                                   name='membership_matching_log',
                                                                                                   if_exists='append',
                                                                                                   index=False)
    else:
        print('No epic_pat_id matches to update')
except:
    error = 'Failed updating Membership table with epic_pat_id matches'
    print(error)
finally:
    phc_db_engine.dispose()
    sys.exit()
