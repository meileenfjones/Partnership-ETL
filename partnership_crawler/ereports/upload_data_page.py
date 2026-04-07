
import time
import os
from datetime import datetime as dt
import pandas as pd
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException


def browse_and_preview_file(browser, file_path, wait_in_sec):
    """
    :param browser:
    :param file_path:
    :param wait_in_sec:
    :return:
    """
    try:
        browser.find_element_by_id("MainContent_fileMeasures").send_keys(file_path)
        browser.find_element_by_id("MainContent_btnPreview").click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


def check_browse_and_preview_error(browser):
    """
    :param browser:
    :return:
    """
    # file validation error occurred
    message = browser.find_element_by_id('MainContent_lblMessage').text.lower()
    if 'error' in message:
        return True
    else:
        return False

    # id=MainContent_lblFreezeUploads
    # error2= browser.find_element_by_class_name('error2').text


def select_measure(browser, name, wait_in_sec):
    """
    :param browser:
    :param name:
    :return:
    """
    try:
        Select(browser.find_element_by_id('MainContent_ddlMeasure')).select_by_value(value)
        # measure_drop_down = browser.find_element_by_id('MainContent_ddlMeasure')
        # measure_chosen = measure_drop_down.find_element_by_xpath('//option[contains(., "{}")]'.format(name))
        # measure_chosen.click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


# View data with no errors
def view_data_with_no_errors(browser, wait_in_sec=2):
    """

    :param browser:
    :param wait_in_sec:
    :return:
    """
    try:
        browser.find_element_by_id('MainContent_btnPreviewWithNoError').click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


def view_data_with_errors(browser, wait_in_sec):
    """

    :param browser:
    :param wait_in_sec:
    :return:
    """
    try:
        browser.find_element_by_id('MainContent_btnPreviewWithError').click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


def upload_data_with_no_errors(browser, wait_in_sec=5):
    """

    :param browser:
    :param wait_in_sec:
    :return:
    """
    try:
        browser.find_element_by_id('MainContent_btnUpload').click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


# Cancel Upload
def cancel_upload(browser, wait_in_sec=2):
    """

    :param browser:
    :param wait_in_sec:
    :return:
    """
    try:
        browser.find_element_by_id('MainContent_btnClearUpload').click()
        time.sleep(wait_in_sec)
        return True
    except NoSuchElementException:
        return False


def check_if_upload_successful(browser):
    """
    :param browser:
    :return:
    """
    message = browser.find_element_by_id('MainContent_lblMessage').text.lower()
    if 'success' in message:
        return True
    else:
        return False


def read_preview_data_results(browser):
    """
    :param browser:
    :return:
    """
    try:
        file_received = browser.find_element_by_id("ctl00_MainContent_rdMeasuresReceived").get_attribute("innerHTML")

        table = pd.read_html(file_received)[0]

        if 'No records' in table.iloc[0, 0]:
            return None
        else:
            return table
    except NoSuchElementException:
        return None


def choose_measure_query(value):
    """
    :param value: html value that maps to the measure drop down option in eReports
    :return: sql query for the measure chosen
    """
    # Adolescent Well Care
    if value == '156':
        return """"""

    # Advance Directive/POLST Submission
    elif value == '157':
        return """"""

    # Asthma Medication Ratio
    elif value == '158':
        return """"""

    # Attestation Submission Template
    elif value == '159':
        return """"""

    # Breast Cancer Screening
    elif value == '160':
        return """"""

    # Cervical Cancer Screening
    elif value == '161':
        return """"""

    # Childhood Immunization Status CIS 10
    elif value == '162':
        return """"""

    # Colorectal Cancer Screening
    elif value == '163':
        return """"""

    # Controlling High Blood Pressure
    elif value == '164':
        return """
            WITH htn_dx_visits AS (
              SELECT
                cin,
                MAX(date) :: DATE AS date
              FROM qip_controlling_blood_pressure_htn_dates
              GROUP BY cin
            )
            SELECT
              php_pts.bic_hik,
              TO_CHAR(bp.bp_date, 'MM/DD/YYYY') AS date,
              bp.systolic,
              bp.diastolic,
              php_pts.clinic,
              php_pts.firstname || ' ' || php_pts.lastname AS patient,
              php_pts.dob
            FROM php_membership AS php_pts
              INNER JOIN php_measures AS msr ON php_pts.bic_hik = msr.bic_hik
              INNER JOIN qip_controlling_high_blood_pressure AS bp ON php_pts.patientid = bp.patient_id
              INNER JOIN htn_dx_visits ON php_pts.bic_hik = htn_dx_visits.cin
            WHERE msr.highbp = 'Denominator' AND
                  bp.numerator = 1 AND
                  bp.bp_date > htn_dx_visits.date AND
                  bp.exclusion = 0
            ORDER BY php_pts.clinic;
        """

    # Diabetes - HbA1C Good Control
    elif value == '165':
        return """"""

    # Diabetes - Retinal Eye Exam
    elif value == '167':
        return """"""

    # Immunization for Adolescents
    elif value == '168':
        return """"""

    # Well Child 3-6 Years
    elif value == '169':
        return """"""

    # Well Child First 15 Months
    elif value == '170':
        return """"""


def make_measure_file(db_engine, directory, file_name, drop_down_value):
    """
    :param db_engine:
    :param directory:
    :param file_name:
    :param drop_down_value:
    :return:
    """
    measure_query = choose_measure_query(drop_down_value)

    df = pd.read_sql(con=db_engine, sql=measure_query)

    # get current date of when the file will be uploaded
    # open the Excel writer
    upload_date = dt.today().strftime('%Y-%m-%d')
    file_path = r'{}\{}_{}.xlsx'.format(directory, file_name, upload_date)
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter')

    # add df to the Excel workbook and open sheet
    df.to_excel(writer, sheet_name=file_name, index=False)
    worksheet = writer.sheets[file_name]

    # add filter option above each column, exclude index column
    worksheet.autofilter(0, 0, df.shape[0], df.shape[1] - 1)

    # dynamically set column width with extra padding
    for i, col in enumerate(df.columns):
        column_len = max(df[col].astype(str).str.len().max(), len(col) + 5)
        worksheet.set_column(i, i, column_len)

    # close the workbook and save file to directory
    writer.save()

    return file_path


def add_sheet_to_measure_file(df, file_path, sheet_name):
    """
    :param df:
    :param file_path:
    :param sheet_name:
    :return:
    """
    try:
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')

        df.to_excel(writer, sheet_name=sheet_name, index=False)

        worksheet = writer.sheets[sheet_name]

        # add filter option above each column, exclude index column
        worksheet.autofilter(0, 0, df.shape[0], df.shape[1] - 1)

        # dynamically set column width with extra padding
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).str.len().max(), len(col) + 5)
            worksheet.set_column(i, i, column_len)

        # save the sheet
        writer.save()

        return True

    except:
        return False


def move_measure_file(measure, measure_file_path, upload_directory):
    # create a directory based on year/measure and then move the
    # file from the measure file path to the new directory in the upload directory

    year = dt.today().strftime('%Y')
    new_dir = r'{}\{}\{}'.format(upload_directory, year, measure)

    measure_file_name = os.path.basename(measure_file_path)
    new_dest = r'{}\{}'.format(new_dir, measure_file_name)

    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

    os.rename(measure_file_path, new_dest)
