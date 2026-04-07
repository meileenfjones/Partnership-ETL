
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


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


def export_to_excel(browser, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :param wait_in_sec:
    :return: True if the export to excel button was clicked
    """
    try:
        web_id = 'ctl00_MainContent_rdMeasuresReceived_ctl00_ctl02_ctl00_ExportToExcelButton'
        button = Wait(browser, wait_in_sec).until(EC.element_to_be_clickable((By.ID, web_id)))
        button.click()
    except TimeoutException:
        return False
    except NoSuchElementException:
        return False
