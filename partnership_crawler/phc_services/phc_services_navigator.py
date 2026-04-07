"""
phc_services_crawler is used as a navigation module to login / logout and go to different pages. This
is meant to be imported with phc_services_..._tools modules that handle different pages
"""

import time
from datetime import datetime as dt
from selenium.webdriver.support.ui import WebDriverWait as Wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException


def login(browser, username, password):
    """
    :param browser: selenium.webdriver being used
    :param username: username to PHC Services Online
    :param password: password to PHC Services Online
    :return: True if able to log into the website successfully
    :resources:
        https://stackoverflow.com/questions/1759455/how-can-i-account-for-period-am-pm-using-strftime
    """
    try:
        login_url = 'https://provider.partnershiphp.org/UI/Login.aspx'
        browser.get(login_url)
        # find section where user fills in credentials
        username_form = browser.find_element(By.ID, "ctl00_contentUserManagement_txtUserName")
        password_form = browser.find_element(By.ID, "ctl00_contentUserManagement_txtPassword")

        # enter credentials and login
        username_form.send_keys(username)
        password_form.send_keys(password)
        browser.find_element(By.ID, "ctl00_contentUserManagement_btnLogin_input").click()

        # check if url has changed after login attempt
        if login_url != browser.current_url:
            login_time = dt.now().strftime("%m/%d/%Y %I:%M %p")
            print('Logged into PHC Online Services: {}'.format(login_time))
            return True
        else:
            print('Unable to log into PHC Online Service. Check credentials.')
            return False
    except NoSuchElementException:
        return False


class ResetPasswordPopUp:

    def __init__(self, browser, wait_in_sec):
        try:
            self.located = True
            # css_selector = 'div[class="ui-dialog ui-widget ui-widget-content ui-corner-all ui-front ui-draggable ui-resizable"]'
            web_id = 'RadWindowWrapper_ctl00_contentUserManagement_modalPopup_rdPopUp'
            self.pop_up = Wait(browser, wait_in_sec).until(EC.visibility_of_element_located((By.ID, web_id)))
        except TimeoutException:
            self.located = False

    def is_displayed(self):
        if not self.located:
            return False
        else:
            return self.pop_up.is_displayed()

    def click_i_understand(self):
        css_selector = 'button[data-bb-handler="cancel"]'
        button = self.pop_up.find_element(By.CSS_SELECTOR, css_selector)
        button.click()

    def click_close(self):
        # css_selector = 'button[class="ui-dialog-titlebar-close"]'
        # button = self.pop_up.find_element_by_css_selector(css_selector)
        web_id = 'ctl00_contentUserManagement_modalPopup_rdPopUp_C_btnRead'
        button = self.pop_up.find_element(By.ID, web_id)
        button.click()


def logout(browser):
    """
    :param browser: selenium.webdriver being used
    :return: True if logged out successfully
    """
    try:
        browser.find_element(By.CLASS_NAME, "dropdown-toggle").click()
        browser.find_element(By.ID,"btnLogout").click()
        logout_time = dt.now().strftime("%m/%d/%Y %I:%M %p")
        print('Logged out of PHC Online Services: {}'.format(logout_time))
        return True
    except NoSuchElementException:
        return False


def to_login_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://provider.partnershiphp.org/UI/Login.aspx')


def to_medo_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://provider.partnershiphp.org/UI/MEDO.aspx')


def to_member_search_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://provider.partnershiphp.org/UI/Membersearch.aspx')


def to_capitation_report_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://provider.partnershiphp.org/UI/CapitationReport.aspx')
