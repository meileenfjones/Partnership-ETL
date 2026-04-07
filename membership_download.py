import sys
import os
import config
import phc_tools
from pykeepass import PyKeePass
import pickle
from partnership_crawler.webdrivers import chrome
from partnership_crawler.phc_services import phc_services_navigator, medo_page
from partnership_processing_toolbox import file_tools
import validation_tools
######################################################################################################################

try:
    # reset the pickles
    if os.path.exists("./membership_download_success.pkl"):
        os.remove("./membership_download_success.pkl")
    if os.path.exists("./membership_process_success.pkl"):
        os.remove("./membership_process_success.pkl")
    # global variables
    download_directory = config.membership_files_download_directory
    phc_database = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH).find_entries(
        title=credentials, first=True)

    browser = chrome.browser(download_directory=str(download_directory), headless=False)

    # log into PHC Services Online

    phc_services_navigator.to_login_page(browser)

    reset_password_setup = phc_services_navigator.ResetPasswordPopUp(browser, wait_in_sec=5)
    if reset_password_setup.is_displayed():
        reset_password_setup.click_close()

    logged_in = phc_services_navigator.login(browser, phc_database.username, phc_database.password)
    if not logged_in:
        raise Exception
except:
    error = 'Error logging into PHC Service website'
    print(error)
    sys.exit()

try:
    # navigate to the monthly eligibility download (medo) page
    phc_services_navigator.to_medo_page(browser)

    # select provider profiles and get page count
    medo_page.select_provider_profiles(browser, wait_in_sec=30)
    provider_profiles_page_count = medo_page.get_provider_profiles_page_count(browser, wait_in_sec=30)

    # for each provider profile page do the following
    for page in range(1, provider_profiles_page_count + 1):

        # navigate to the page
        medo_page.to_provider_profiles_page(browser, page, wait_in_sec=30, wait_to_load=70)

        # get table row web elements that contain RHC
        rhc_tr_web_elements = medo_page.get_tr_web_elements(browser, tr_contains=['RHC'])

        # for each RHC row web element do the following
        for rhc_tr_web_element in rhc_tr_web_elements:

            # get the row text
            rhc_tr_text = medo_page.get_tr_text(rhc_tr_web_element)

            # get the table header web elements for the following member types
            member_types = ['Capitated', 'Special', 'CCS']
            member_th_web_elements = medo_page.get_th_web_elements(rhc_tr_web_element, th_contains=member_types)

            # for each member type header web element available do the following
            for member_th_web_element in member_th_web_elements:

                # get the member type header and view the members
                member_type = medo_page.get_member_type(member_th_web_element, member_types)
                medo_page.view_members(browser, member_th_web_element, load_in_sec=120)

                # check the extended format if not already checked
                if medo_page.is_extended_format_box_checked(browser, wait_in_sec=20) is False:
                    medo_page.check_extended_format_box(browser)

                # if no members available move to the next row header web element
                members_count = medo_page.get_medo_count(browser, wait_in_sec=30)
                if members_count == 0:
                    continue
                # get the download month year selected to use as a directory
                medo_year = medo_page.get_medo_year(browser)
                medo_month = medo_page.get_medo_month(browser)

                # download in excel and wait for the download to complete for up to 2 minutes
                medo_page.download_in_excel(browser, wait_in_sec=10)
                file_tools.wait_for_file_to_download(str(download_directory), wait_in_sec=120,
                                                     name=member_type + ' ' + rhc_tr_text)

                # get the most recently downloaded file path
                medo_file_path = file_tools.get_last_file_path_modified(str(download_directory), walk=False)
                medo_file_name = os.path.basename(medo_file_path)

                member_type_medo_file_name = f'{member_type}-{medo_file_name}'
                member_type_medo_file_path = os.path.join(str(download_directory), member_type_medo_file_name)

                os.rename(medo_file_path, member_type_medo_file_path)

                # move the downloaded file to a new directory based off the year, month, and member type
                output_directory = download_directory.joinpath(medo_year).joinpath(medo_month)
                file_tools.move_file(member_type_medo_file_path, str(output_directory))
            # move to the next row
        # move to the next page
    phc_services_navigator.logout(browser)
    browser.quit()
except:
    error = 'Error downloading Membership files from PHC Service website'
    print(error)
    phc_services_navigator.logout(browser)
    browser.quit()
    sys.exit()

try:
    # connect to partnership database to query active clinic values
    phc_db_conn = phc_tools.SQL_connection(keepass_filename=config.KDBX_FILE, keepass_keyfile=config.KEY_PATH)
    active_clinic_phc_ids = phc_tools.active_values_query(db_connection=phc_db_conn, return_type='list',
                                                          table_name='clinic_map', query_col_1='phc_id')
    phc_db_conn.close()
except:
    error = 'Failed querying active values from partnership database'
    print(error)
    phc_db_conn.close()
    sys.exit()

# validate file names to make sure no files are associated to unmapped clinics and all clinics have files
try:
    if validation_tools.file_counter(path=output_directory, extension='xls') == 0:
        raise Exception('Files not properly exported to correct location')
    if not validation_tools.file_name_clinic_validation(path=output_directory, extension='xls',
                                                        active_clinics=active_clinic_phc_ids,
                                                        delimiter='-', delimiter_index=1):
        missing_clinics = validation_tools.clinics_without_files(path=output_directory, extension='xls',
                                                                 active_clinics=active_clinic_phc_ids,
                                                                 delimiter='-', delimiter_index=1)
        unmapped_clinics = validation_tools.unmapped_clinic_files(path=output_directory, extension='xls',
                                                                  active_clinics=active_clinic_phc_ids,
                                                                  delimiter='-', delimiter_index=1)
        if missing_clinics['result'] & unmapped_clinics['result']:
            validation_error_message = missing_clinics['message'] + ' & ' + unmapped_clinics['message']
        elif missing_clinics['result']:
            validation_error_message = missing_clinics['message']
        else:
            validation_error_message = unmapped_clinics['message']
        raise Exception(validation_error_message)
    # pickle success
    with open("./membership_download_success.pkl", 'wb') as file:
        pickle.dump(True, file)
    print('Success completing & validating Membership Download')
    sys.exit()
except Exception as error:
    print(error)
    sys.exit()
