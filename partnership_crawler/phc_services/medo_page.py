"""
phc_services_medo_page_tools is used to interact with the PHC Services Online - Monthly Eligibility Download
page. Functions mimic human interaction with the page and use Xpath to find the web elements that
can be clicked / viewed. This is meant to be imported alongside the phc_services_crawler module
"""

import re
from datetime import datetime as dt
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.common.exceptions import NoSuchElementException, TimeoutException


def select_provider_profiles(browser, wait_in_sec):
    """
    To be used immediately after navingating to the MEDO page to view members
    :param browser: selenium.webdriver being used
    :param wait_in_sec: seconds to wait until Provider Profiles button can be clicked
    :return: True if the Provider Profiles was clicked
    """
    try:
        web_id = 'ctl00_ContentPlaceHolder1_ProviderProfile_btnSelectProviders_input'
        provider_profiles = Wait(browser, wait_in_sec).until(EC.element_to_be_clickable((By.ID, web_id)))
        provider_profiles.click()
        return True
    except NoSuchElementException:
        return False


def get_provider_profiles_page_count(browser, wait_in_sec):
    """
    To be used after Provider Profiles has been clicked
    :param browser: selenium.webdriver being used
    :param wait_in_sec:
    :return: page count of Provider Profiles (observed 3 rows of info per page)
    """
    try:
        explicit_wait = Wait(browser, wait_in_sec)
        xpath = '//div[@id="ctl00_ContentPlaceHolder1_rdMEDOCounts"]//div[@class="rgWrap rgInfoPart"]'
        explicit_wait.until(EC.text_to_be_present_in_element((By.XPATH, xpath), 'Page'))
        page_info = browser.find_element(By.XPATH, xpath).text.strip().lower()
        page_count = re.search(r'^page \d+ of (\d+)', page_info).group(1)
        return int(page_count)
    except NoSuchElementException:
        return 1


def to_provider_profiles_page(browser, page, wait_in_sec, wait_to_load):
    """
    To be used to get the number of Provider Profiles pages to loop through
    :param browser: selenium.webdriver being used
    :param page: page number to navigate to in the Provider Profiles section
    :param wait_in_sec: seconds to wait until page can be clicked
    :return: True if the page number was clicked
    """
    try:
        explicit_wait = Wait(browser, wait_in_sec)
        xpath = '//div[@class="rgWrap rgNumPart"]//span[.="{}"]'.format(page)
        provider_profile_page = explicit_wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        provider_profile_page.click()

        xpath2 = '//div[@class="rgWrap rgNumPart"]//a[@class="rgCurrentPage"]'
        Wait(browser, wait_to_load).until(EC.text_to_be_present_in_element((By.XPATH, xpath2), str(page)))

        return True
    except NoSuchElementException:
        return False
    except TimeoutException:
        raise Exception('Provider Profile page {} did not load in time'.format(page))


def get_tr_web_elements(browser, tr_contains):
    """
    To be used after the Provider Profiles has been clicked. At a given Provider Profiles page
    any rows that contain a given piece of text will be captured and used to view members
    :param browser: selenium.webdriver being used
    :param tr_contains: list of text to be used to capture rows in the Provider Profiles section / currently "RHC" only
    :return: list of selenium.webdriver.webelement
    """
    try:
        contains_elements = ' or '.join(['contains(., "{}")'.format(element) for element in tr_contains])
        xpath = '//div[@id="ctl00_ContentPlaceHolder1_rdMEDOCounts"]/table/tbody/tr[{}]'.format(contains_elements)
        tr_web_elements = browser.find_elements(By.XPATH, xpath)
        return tr_web_elements
    except NoSuchElementException:
        return None


def get_tr_text(tr_web_element):
    """
    To be used to get the text of the row that members will be viewed
    :param tr_web_element: selenium.webdriver.webelement returned from get_rhc_tr_web_elements()
    :return: text of the row from Provider Profiles section
    """
    try:
        # get all td web elements from the tr web element
        td_web_elements = tr_web_element.find_elements(By.TAG_NAME, 'td')
        # get all td text in a list, exclude blank td
        td_text_list = [re.sub(r'\s+', ' ', td.text) for td in td_web_elements if td.text != '']
        # join td text into one string separated by |
        tr_text = ' | '.join(td_text_list)
        return tr_text
    except NoSuchElementException:
        return None


def get_th_web_elements(tr_web_element, th_contains):
    """
    :param tr_web_element: selenium.webdriver.webelement returned from get_tr_web_elements()
    :param th_contains: list of member headers
    :return: list of selenium.webdriver.webelement
    """
    try:
        contains_elements = ' or '.join(['contains(@id, "{}")'.format(element) for element in th_contains])
        xpath = './/a/input[{}]'.format(contains_elements)
        th_web_elements = tr_web_element.find_elements(By.XPATH, xpath)
        return th_web_elements
    except NoSuchElementException:
        return None


def get_member_type(th_web_element, member_types):
    """
    :param th_web_element:
    :param member_types:
    :return:
    """
    for member_type in member_types:
        if member_type in th_web_element.get_attribute('id'):
            return member_type
        else:
            continue


def view_members(browser, th_web_element, load_in_sec):
    """
    To be used to view the members at a given row and header
    :param browser: selenium.webdriver being used
    :param th_web_element: selenium.webdriver.webelement returned from get_th_web_elements()
    :param load_in_sec: seconds to wait after viewing the members
    :return: True if the members were clicked to be viewed
    """
    try:
        th_web_element.click()
        load = Wait(browser, load_in_sec)
        load.until(EC.invisibility_of_element((By.CSS_SELECTOR, "div.raDiv")))
        return True
    except NoSuchElementException:
        return False


def is_extended_format_box_checked(browser, wait_in_sec):
    """
    To be used to verify if the extended format button should then be clicked after viewing members
    :param browser: selenium.webdriver being used
    :param wait_in_sec: seconds to wait after checking the extended format box
    :return: True if the extended format box is checked
    """
    try:
        web_id = 'ctl00_ContentPlaceHolder1_rdMEDO_ctl00_ctl02_ctl00_chkExtended'
        extended_format_box = Wait(browser, wait_in_sec).until(EC.element_to_be_clickable((By.ID, web_id)))
        return extended_format_box.is_selected()
    except NoSuchElementException:
        return False


def check_extended_format_box(browser):
    """
    To be used to get the data in a extended view Ex) Address -> Street, City, State, etc..
    after there is a check that the extended format box is checked
    :param browser: selenium.webdriver being used
    :return: True if the extended format box is checked
    """
    try:
        web_id = 'ctl00_ContentPlaceHolder1_rdMEDO_ctl00_ctl02_ctl00_chkExtended'
        browser.find_element(By.ID, web_id).click()
        return True
    except NoSuchElementException:
        return False


def get_download_month_year_selected(browser):
    """
    To be used to get the Month-Year selected for viewing or downloaded members
    :param browser: selenium.webdriver being used
    :return: the month - year of the members being viewed or downloaded
    """
    try:
        web_id = 'ctl00_ContentPlaceHolder1_rdMEDO_ctl00_ctl02_ctl00_rdlMonth'
        month_year_drop_down = browser.find_element(By.ID, web_id)
        month_year = month_year_drop_down.find_element(By.XPATH, '//option[@selected="selected"]').text
        return month_year
    except NoSuchElementException:
        return None


def get_medo_month(browser):
    """
    To be used for moving the medo file to a directory based on month downloaded
    :param browser: selenium.webdriver being used
    :return: the month the members being viewed or downloaded
    """
    month_year = get_download_month_year_selected(browser)
    month_year_split = month_year.split(' - ')
    month = '{:02d}'.format(dt.strptime(month_year_split[0], '%B').month)
    return month


def get_medo_year(browser):
    """
    To be used for moving the medo file to a directory based on year downloaded
    :param browser: selenium.webdriver being used
    :return: the year the members being viewed or downloaded
    """
    month_year = get_download_month_year_selected(browser)
    month_year_split = month_year.split(' - ')
    year = month_year_split[1]
    return year


def get_medo_count(browser, wait_in_sec):
    """
    To be used to check how many members will be downloaded after clicking View in the Provider Profiles section
    :param browser: selenium.webdriver being used
    :param wait_in_sec: seconds to wait until the medo count appears
    :return: the count of members that can be downloaded
    """
    try:
        explicit_wait = Wait(browser, wait_in_sec)
        xpath = '//div[@id="ctl00_ContentPlaceHolder1_ctl00_ContentPlaceHolder1_rdMEDOPanel"]//div[@class="rgWrap rgInfoPart"]'
        explicit_wait.until(EC.text_to_be_present_in_element((By.XPATH, xpath), 'Page'))
        page_info = browser.find_element(By.XPATH, xpath).text.strip().lower()
        medo_count = re.search(r'of (\d+).$', page_info).group(1)
        return int(medo_count)
    except NoSuchElementException:
        return 0


def download_in_excel(browser, wait_in_sec):
    """
    :param browser: selenium.webdriver being used
    :param wait_in_sec: seconds to wait until the download in excel button can be clicked
    :return: True if button was clicked
    """
    try:
        explicit_wait = Wait(browser, wait_in_sec)
        web_id = 'ctl00_ContentPlaceHolder1_rdMEDO_ctl00_ctl02_ctl00_DownloadXLSX'
        download_excel = explicit_wait.until(EC.element_to_be_clickable((By.ID, web_id)))
        download_excel.click()
        return True
    except NoSuchElementException:
        return False
