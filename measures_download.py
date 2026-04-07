import os
import sys
import config
from datetime import datetime as dt
from pykeepass import PyKeePass
import pickle
from partnership_crawler.webdrivers import chrome
from partnership_crawler.ereports import ereports_navigator, member_report_page
from partnership_processing_toolbox import file_tools
import validation_tools
#################################################################

# do not start download measures process if not past the 10th day of the month
if dt.today().day < 10:
    print(f"Partnership-ETL Measures Download: Not yet the 10th day of the given month")
    sys.exit()

# global variables
try:
    if os.path.exists("./measures_download_success.pkl"):
        os.remove("./measures_download_success.pkl")
    if os.path.exists("./measures_upload_success.pkl"):
        os.remove("./measures_upload_success.pkl")

    download_directory = config.ereports_file_download_directory
    processed_directory = config.ereports_file_processed_directory
    ereports_database = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH).find_entries(title=ereports_credentials
                                                                                                   first=True)
    print('Made it through the credentials')
except:
    error = 'Failed getting credentials'
    print(error)
    sys.exit()

try:
    # open up chrome browser with adjusted settings

    browser = chrome.browser(download_directory=str(download_directory), headless=True)
    print('Made it through the chrome browser')
except:
    error = 'Failed getting through chrome browser'
    print(error)
    sys.exit()

# log into QIP eReports
try:
    logged_in = ereports_navigator.login(browser, ereports_database.username, ereports_database.password)
    if not logged_in:
        raise Exception
    print('Successfully logged into QIP eReports')
except:
    error = 'Failed logging in to QIP eReports'
    print(error)
    browser.quit()
    sys.exit()

try:
    # loading Member Report Page
    ereports_navigator.to_member_report_page(browser)
    # get when qip scores were last updated
    qip_scores_updated_on_date = member_report_page.get_scores_updated_on(browser, wait_in_sec=5)
    if qip_scores_updated_on_date is None:
        raise Exception
except:
    error = 'Unable to determine when QIP scores were last updated'
    print(error)
    ereports_navigator.logout(browser)
    browser.quit()
    sys.exit()

try:
    # get last qip processed file date
    last_qip_file_dir = file_tools.get_last_file_path_modified(str(processed_directory), walk=True)
    last_qip_file_processed_date = file_tools.get_file_modified_date(last_qip_file_dir, data_type_return=dt)
    # if qip scores updated is more recent than qip file last processed then download a new file, otherwise skip
    if not qip_scores_updated_on_date > last_qip_file_processed_date:
        raise Exception
    print('There are updated QIP Scores')
except:
    error = 'QIP Scores not updated yet, skipping download'
    print(error)
    ereports_navigator.logout(browser)
    browser.quit()
    sys.exit()

try:
    # wait up to 2 minutes for the download to complete
    member_report_page.export_to_excel(browser, wait_in_sec=5)
    file_tools.wait_for_file_to_download(str(download_directory), wait_in_sec=120)
    print('Exported to Excel')
    ereports_navigator.logout(browser)
    browser.quit()
except:
    error = 'Failed exporting and downloading file'
    print(error)
    ereports_navigator.logout(browser)
    browser.quit()
    sys.exit()

try:
    # get the path of the exported file
    xls_file_path = max(file_tools.get_file_paths_list(str(download_directory), walk=False), key=os.path.getmtime)
    print('Downloaded file was found here: ' + xls_file_path)

    # get the file month and year it was downloaded and rename the file
    download_date = file_tools.get_file_modified_date(xls_file_path, data_type_return=str)
    file_month = file_tools.get_file_modified_month(xls_file_path, data_type_return=int)
    file_year = file_tools.get_file_modified_year(xls_file_path, data_type_return=int)
    qip_file_name = f'QIP_{file_month:02d}-{file_year}_{download_date}.xls'
    os.rename(xls_file_path, os.path.join(str(download_directory), qip_file_name))
    print(f'Saved Exported file and renamed to {qip_file_name}\n')
    if validation_tools.file_counter(path=download_directory, extension='xls') == 0:
        raise Exception
    # pickle success
    with open("./measures_download_success.pkl", 'wb') as file:
        pickle.dump(True, file)
except:
    error = 'Failed saving and renaming file'
    print(error)
finally:
    sys.exit()
