# Data Definitions & ERD


## clinic_map
All clinics Partnership assigns patients to  
*Rows are manually added/updated as needed*

| column     | description                                                                                                   | data type   |
|------------|---------------------------------------------------------------------------------------------------------------|-------------|
| id         | Primary Key: unique id of row                                                                                 | INTEGER     |
| phc_id     | Partnership assigned id used to distinguish clinics                                                           | INTEGER     |
| epic_id    | A reference to the OCHIN id for the given clinic                                                              | INTEGER     |
| clinic_key | An abbreviation for the clinic which is referenced in OCHIN EPIC                                              | VARCHAR(20) |
| clinic     | The full name of the clinic                                                                                   | VARCHAR(50) |
| county     | The county where the clinic is located                                                                        | VARCHAR(50) |
| is_active  | 0 = inactive clinic where patients were once assigned<br/>1 = active clinic where patients are being assigned | BIT         |


## measures_map
All quality measures Partnership tracked for the QIP Program  
*Rows are manually added/updated as needed*

| column      | description                                                                                           | data type    |
|-------------|-------------------------------------------------------------------------------------------------------|--------------|
| id          | Primary Key: unique id of row                                                                         | INTEGER      |
| measure_key | The shortened/abbreviated name for the quality measure and is used in the ETL file naming process     | VARCHAR(50)  |
| measure     | The name of the quality measure as seen on the QIP Member Report file download                        | VARCHAR(100) |
| is_active   | 0 = inactive quality measure no longer tracked for QIP<br/>1 = active quality measure tracked for QIP | BIT          |


## membership
Holds the Partnership monthly eligible download files   
*Refer to .py files on how the table gets populated*

| column          | description                                                                                                                                                                                       | data type     |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| cin             | Primary Key: California Identification Number from Partnership and unique per patient                                                                                                             | CHAR(10)      |
| mpi             | A reference to custom.master_patient_ids                                                                                                                                                          | INTEGER       |
| epic_pid        | A reference to the OCHIN patient_id                                                                                                                                                               | INTEGER       |
| clinic_id       | A reference to the clinic_map.id                                                                                                                                                                  | INTEGER       |
| first_name      | The patients first name up to 13 characters from Partnership                                                                                                                                      | VARCHAR(50)   |
| last_name       | The patients first name up to 13 characters from Partnership                                                                                                                                      | VARCHAR(50)   |
| dob             | The patients date of birth from Partnership                                                                                                                                                       | DATE          |
| age             | A calculated column that returns the current age of a patient as an integer                                                                                                                       | INTEGER       |
| sex             | Sex of patient from Partnership                                                                                                                                                                   | VARCHAR(12)   |
| phone_number    | Phone number of patient                                                                                                                                                                           | VARCHAR(12)   |
| address         | Address of patient residence from Partnership                                                                                                                                                     | VARCHAR(500)  |
| city            | City of patient residence from Partnership                                                                                                                                                        | VARCHAR(500)  |
| state           | State of patient residence from Partnership                                                                                                                                                       | VARCHAR(20)   |
| zip_code        | Zip code of patient residence from Partnership                                                                                                                                                    | VARCHAR(10)   |
| record_number   | UNKNOWN: from Partnership \| historically we have included this column in case needed                                                                                                             | VARCHAR(20)   |
| rp              | UNKNOWN: from Partnership \| historically we have included this column in case needed                                                                                                             | VARCHAR(4)    |
| eff_date        | Effective date of enrollment of patient from Partnership                                                                                                                                          | DATE          |
| end_date        | End date of enrollment of patient from Partnership                                                                                                                                                | DATE          |
| other_insurance | UNKNOWN: from Partnership \| historically we have included this column in case needed                                                                                                             | VARCHAR(4)    |
| new_member      | If a patient is considered a "new member" when the Membership file as downloaded from Partnership                                                                                                 | BIT           |
| date_new_member | The date a patient became a new member according to Partnership from new_member or if a patient was first added to the table                                                                      | DATE          |
| months_enrolled | The total number of months enrolled in a given calendar year that count towards being QIP eligible                                                                                                | INTEGER       |
| months_missed   | The total number of months missed in a given calendar year that count towards being QIP eligible                                                                                                  | INTEGER       |
| is_enrolled     | 0 = The patient is not currently enrolled in Partnership based off download_date<br/>1 = The patient is currently enrolled based off the download_date                                            | BIT           |
| is_qip_eligible | 0 = Patient is not QIP eligible based off months enrolled and anchor month<br/>1 = Patient is QIP Eligible based based off months enrolled and anchor month                                       | BIT           |
| download_date   | The date the Membership file was downloaded and used to update/insert new patients into the database                                                                                              | DATE          |
| updated_at      | The datetime of when a row was last updated - it will usually be the date of the download_date (initialized at 2024-06-10 14:57:00 for June run and historical data with missing download_date)   | SMALLDATETIME |


## membership_history
Holds the year-month history for each patient that appeared in the Membership file  
*Refer to .py files on how the table gets populated*

| column          | description                                                                                                                                                          | data type     |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| id              | Incrementing column starting at 1                                                                                                                                    | INTEGER       |
| cin             | References membership.cin                                                                                                                                            | CHAR(10)      |
| clinic_id       | References clinic_map.id                                                                                                                                             | INTEGER       |
| year            | The year the patient was enrolled                                                                                                                                    | INTEGER       |
| month           | The month the patient was enrolled                                                                                                                                   | INTEGER       |
| is_qip_eligible | 0 = Not QIP eligible for the given year-month <br/>1 = QIP Eligible for the given year-month<br/>_NOTE: Comes from membership.is_qip_eligible at the time of upload_ | BIT           |
| download_date   | The date the Membership file was downloaded from Partnership                                                                                                         | DATE          |
| inserted_at     | The datetime the row of data was inserted into the table                                                                                                             | SMALLDATETIME |

Primary Key (cin, year, month)


## measures_history
Holds the quality measure results from the eReports Member Report files  

NOTE: Rows of historical and redundant data will be deleted on the 2nd Sunday of each month only keeping
the most recent rows at the end of a given year-month for a measure. This is done as a job via the SQL Server Agent.

*Refer to .py files on how the table gets populated and rows deleted*

| column           | description                                                                                                                                                          | data type     |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| id               | Primary Key: unique id of row                                                                                                                                        | INTEGER       |
| cin              | References membership.cin                                                                                                                                            | CHAR(10)      |
| clinic_id        | References clinic_map.id                                                                                                                                             | INTEGER       |
| measure_id       | References measures_map.id                                                                                                                                           | INTEGER       |
| year             | The year the patient was in the eReports download file. We assume they are enrolled at that time.                                                                    | INTEGER       |
| month            | The month the patient was in the eReports download file. We assume they are enrolled at that time.                                                                   | INTEGER       |
| is_compliant     | 0 = Not compliant for a measure at the download date <br/>1 = Compliant for a measure at the download date                                                           | BIT           |
| is_qip_eligible  | 0 = Not QIP eligible for the given year-month <br/>1 = QIP eligible for the given year-month<br/>_NOTE: Comes from membership.is_qip_eligible at the time of upload_ | BIT           |
| download_date    | The date the eReports file was downloaded from Partnership                                                                                                           | DATE          |
| inserted_at      | The datetime the row of data was inserted into the table                                                                                                             | SMALLDATETIME |


## measures_summary_history
Holds an aggregated snapshot of the quality measure results after there is an insert into measures_history. This is done via
the trigger: _measures_summary_history_insert_

Historical data

| column           | description                                                                                                    | data type     |
|------------------|----------------------------------------------------------------------------------------------------------------|---------------|
| id               | Primary Key: unique id of row                                                                                  | INTEGER       |
| clinic_id        | References clinic_map.id                                                                                       | INTEGER       |
| measure_id       | References measures_map.id                                                                                     | INTEGER       |
| year             | The year the patient was in the eReports download file. We assume they are enrolled at that time.              | INTEGER       |
| month            | The month the patient was in the eReports download file. We assume they are enrolled at that time.             | INTEGER       |
| qip_numerator    | The numerator aggregated and filtered to only QIP eligible for a given measure and clinic at the download_date | INTEGER       |
| qip_denominator  | The denominator aggregated filtered to only QIP eligible for a given measure and clinic at the download_date   | INTEGER       |
| numerator        | The full aggregated denominator for a given measure and clinic at the download_date                            | INTEGER       |
| denominator      | The full aggregated denominator for a given measure and clinic at the download_date                            | INTEGER       |
| download_date    | The date the eReports file was downloaded from Partnership                                                     | DATE          |
| inserted_at      | The datetime the row of data was inserted into the table                                                       | SMALLDATETIME |


## measure_goals

The measure goals can be found in the Final QIP Specifications from Partnership. Data should be added in manually

| column     | description                                                       | data type |
|------------|-------------------------------------------------------------------|-----------|
| id         | Primary Key: unique id of row                                     | INTEGER   |
| measure_id | References measures_map.id                                        | INTEGER   |
| year       | The year of the measure goal                                      | INTEGER   |
| percentage | The full points target percentage for the measure at a given year | DECIMAL   |


## etl_membership_log

A log table used to refer how many rows were recorded during an update/insert to the membership table

| column             | description                                                                          | data type     |
|--------------------|--------------------------------------------------------------------------------------|---------------|
| id                 | Primary Key: unique id of row                                                        | INTEGER       |
| logged_at          | The datetime the row of information was logged                                       | SMALLDATETIME |
| year               | The year the membership table was updated/inserted into                              | INTEGER       |
| month              | The month the membership table was updated/inserted into                             | INTEGER       |
| update_count       | The total number of rows updated in membership table                                 | INTEGER       |
| insert_count       | The total number of rows inserted into membership table                              | INTEGER       |
| enrolled_count     | The total number of patients enrolled in membership table                            | INTEGER       |
| qip_eligible_count | The total number of QIP eligible patients in membership table                        | INTEGER       |
| failure_step       | The step in the Python script that threw an error and stopped successful logging     | VARCHAR(MAX)  |
| is_success         | 0 = Failure completing the etl process<br/>1 = Success in completing the etl process | BIT           |


## etl_measures_log

A log table used to refer how many rows of data were added/deleted when the eReports data is downloaded and used for tracking

| column          | description                                                                          | data type     |
|-----------------|--------------------------------------------------------------------------------------|---------------|
| id              | Primary Key: unique id of row                                                        | INTEGER       |
| logged_at       | The datetime the row of information was logged                                       | SMALLDATETIME |
| file_name       | The eReports file name that was used to update the measures_history table            | VARCHAR(100)  |
| download_date   | The date the eReports file was downloaded                                            | DATE          |
| insert_count    | The total number of rows that was added to the measures_history table                | INTEGER       |
| delete_count    | The total number of rows deleted from the measures_history table                     | INTEGER       |
| warning_message | Any potential warning message in the Python script that creates the log              | VARCHAR(MAX)  |
| failure_step    | The step in the Python script that threw an error and stopped successful logging     | VARCHAR(MAX)  |
| is_success      | 0 = Failure completing the etl process<br/>1 = Success in completing the etl process | BIT           |

