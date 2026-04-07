# Partnership ETL

__Background__  
This project supports a clinic’s data needs for outreach, operations, and revenue optimization tied to the Partnership Quality Improvement Program (QIP) — a core revenue driver. Under this program, financial incentives are tied to achieving targeted performance percentages across several QIP measures, making timely, accurate data essential for operational planning, reporting, and incentive realization.

__Deliverables__  
The final product will be a scalable database properly housing all relevant information about these Partnership members and able to support historical and current reporting. 

## Prerequisites

List all items needed for the program to work

- Windows OS
- Task Scheduler
- KeePass database
    - database credentials stored securely
- Python (3.11.3)
  - pandas>=2.0.0 
  - SQLAlchemy>=2.0.8 
  - pykeepass>=4.0.3 
  - psycopg2>=2.9.6 
  - prefect==2.11.2 
  - rapidfuzz>=3.2.0 
  - pyodbc>=4.0.32

### Setup

List all packages/technologies needed to build the program

1. Download most recent python on C drive [latest version for Windows](https://www.python.org/downloads/)
2. Download the [latest release]) and unzip.
3. Open a Windows Command Prompt Terminal
4. Change directory to the script folder

  ```
  cd Partnership-ETL
  ```

5. Create virtual environment

  ```
  python -m venv venv
  ```

6. Activate the virtual environment

  ```
  venv\Scripts\activate.bat
  ```

7. Install Python packages

  ```
  pip install -r requirements.txt
  ```

8. Create config.py and fill out with following paths

```
from pathlib import Path
############################################################################################################
# specified keepass file and key path for this project

KDBX_FILE = 
KEY_PATH =

############################################################################################################

```

9. Create/verify database schema and tables as well as logging table

10. To automate project, create tasks with Windows Task Scheduler directed at the bat files set to run at specified times


## Documentation

### Files

List important files to know about and how they work together

`config.py`
- Contains paths for KeePass file, key path, and file export folder for monthly unmapped users

`analytics_platform_connection.py`
- Given kdbx file access, pulls credentials to create, test, and kill the database connection
- used to log the changes to Membership table

`membership_download.py`
- Script to go to Partnership site and download the current roster of Members

`membership_process.py`
- Script to clean, transform, and validate data into structured files per clinic/month

`membership_upload.py`
- Script to upload cleaned data into Membership table

`measures_download.py`
- Download the current measures data from eReports site

`measures_upload.py`
- Transform and integrate new data into Measures database

`membership_patient_match.py`
- Given kdbx file access, pulls credentials to connect to SERVER database
- will match unmapped Partnership members with their EHR id


### Methodology

1. Uses PyKeePass to connect to kdbx database for credentials
2. connects to SERVER for database connection
3. Goes to Partnership erepoorts site and integrates current monthly member list into database
4. Updates flag for who will be eligible for the Quality Improvement Program (QIP) for the year
5. Matches Partnership members to their EHR patient id using birth date and fuzzy matches
6. Downloads new Measure data twice a week and integrates current measure standings into analytics dashboard

