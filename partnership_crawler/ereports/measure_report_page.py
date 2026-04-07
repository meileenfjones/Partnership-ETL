
import time
import pandas as pd
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


def select_measure(browser, measure, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :param measure: the value
    :param wait_in_sec:
    :return:
    """
    try:
        web_id = 'MainContent_ddlMeasure'
        options = Wait(browser, wait_in_sec).until(EC.presence_of_element_located((By.ID, web_id)))

        try:
            xpath = '//option[contains(., "{}")]'.format(measure)
            # value_id = options.find_element_by_xpath(xpath).get_attribute('value')
            value_id = options.find_element(By.XPATH, xpath).get_attribute('value')
            Select(options).select_by_value(value_id)
            return True
        except NoSuchElementException:
            return False

    except NoSuchElementException:
        return False


def select_pcp(browser, pcp, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :param pcp: the PHC Clinic ID
    :param wait_in_sec:
    :return: True if PCP could be selected
    """

    web_id1 = 'ctl00_MainContent_ddlGroups_Arrow'
    web_id2 = 'ctl00_MainContent_ddlGroups_DropDown'
    content_xpath = f'div[@class = "rcbScroll rcbWidth"]/ul[@class = "rcbList"]//li[contains(., "{pcp}")]'
    checkbox_xpath = 'label/input[@class = "rcbCheckBox"]'

    try:
        # identify drop down
        drop_down = Wait(browser, wait_in_sec).until(EC.element_to_be_clickable((By.ID, web_id1)))
        drop_down.click()
        time.sleep(1)
        # re-identify drop down with content
        drop_down_content = Wait(browser, wait_in_sec).until(EC.presence_of_element_located((By.ID, web_id2)))
        content = drop_down_content.find_element(By.XPATH, content_xpath)
        # find the checkbox to click
        checkbox = content.find_element(By.XPATH, checkbox_xpath)
        # click the checkbox / close the dropdown and wait
        checkbox.click()
        time.sleep(1)
        drop_down.click()
        time.sleep(1)
        return True
    except NoSuchElementException:
        return False


def export_to_excel_with_data_sources(browser, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :return: True if download completed or exceeded time
    """
    try:
        web_id = 'MainContent_imgExporttoExcelwithDS'
        button = Wait(browser, wait_in_sec).until(EC.element_to_be_clickable((By.ID, web_id)))
        button.click()
        return True
    except NoSuchElementException:
        return False


def get_scores_updated_on(browser, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :param wait_in_sec:
    :return: date the qip members score was last updated
    """
    try:
        web_id = 'lblUpdatedOn'
        updated_on = Wait(browser, wait_in_sec).until(EC.presence_of_element_located((By.ID, web_id)))
        return pd.to_datetime(updated_on.text)
    except TimeoutException:
        return None
    except NoSuchElementException:
        return None
