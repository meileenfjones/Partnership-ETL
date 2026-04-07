
"""
ereports_crawler is used as a navigation module to login / logout and go to different pages. This
is meant to be imported with ereports_..._tools modules that handle different pages
"""

from datetime import datetime as dt
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

def login(browser, username, password):
    """
    :param browser: selenium.webdriver being used
    :param username: username to QIP eReports
    :param password: password to QIP eReports
    :return: True if able to log into the website successfully
    https://stackoverflow.com/questions/1759455/how-can-i-account-for-period-am-pm-using-strftime
    """
    try:
        login_url = 'https://qip.partnershiphp.org/Login.aspx'
        browser.get(login_url)

        # find section where user fills in credentials
        username_form = browser.find_element(By.ID, "txtUserName")
        password_form = browser.find_element(By.ID, "txtPassword")

        # enter credentials and login
        username_form.send_keys(username)
        password_form.send_keys(password)

        browser.find_element(By.ID, "btnLogIn").click()

        # check if url has changed after login attempt
        if login_url != browser.current_url:
            login_time = dt.now().strftime("%m/%d/%Y %I:%M %p")
            print('Logged into eReports: {}'.format(login_time))
            return True
        else:
            print('Unable to log into eReports. Check credentials.')
            return False
    except NoSuchElementException:
        return False


def logout(browser):
    """
    :param browser: selenium.webdriver being used
    :return: True if logged out successfully
    """
    try:
        browser.find_element(By.ID, "btnLogout").click()
        logout_time = dt.now().strftime("%m/%d/%Y %I:%M %p")
        print('Logged out of eReports: {}'.format(logout_time))
        return True
    except NoSuchElementException:
        return False


def to_member_report_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://qip.partnershiphp.org/CodeLevelQIPMemberReports.aspx')


def to_measure_report_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://qip.partnershiphp.org/ViewScores.aspx')


def to_member_search_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://qip.partnershiphp.org/membersearch.aspx')


def to_upload_data_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://qip.partnershiphp.org/UploadQIPData.aspx')


def to_pqd_page(browser):
    """
    :param browser: selenium.webdriver being used
    :return: None
    """
    browser.get('https://qip.partnershiphp.org/PQDDashboard.aspx')
