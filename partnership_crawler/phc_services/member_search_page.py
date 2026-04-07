import time
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By


def search_cin(browser, cin):
    try:
        # wait for page to load and find the form to enter the CIN number
        time.sleep(2)
        form = browser.find_element(By.ID, "ctl00_ContentPlaceHolder1_ucSearchMember_rdCin")
    except NoSuchElementException:
        return False

    try:
        # clear any past results and enter the CIN number
        form.clear()
        form.send_keys(cin)
        # wait for page to refresh and search member
        time.sleep(1.5)
        browser.find_element(By.ID, "ContentPlaceHolder1_ucSearchMember_btnSearch").click()
        time.sleep(2)
        return True
    except NoSuchElementException:
        return False


def click_search_result(browser):
    try:
        # click on the member in the result and wait for page to load
        browser.find_element(By.ID,
                             "ctl00_ContentPlaceHolder1_ucSearchMember_rdGridMember_ctl00_ctl04_lnkAction").click()
        time.sleep(2)
        return True
    except NoSuchElementException:
        return False


def pop_up_alert_then_close(browser):
    try:
        # if the patient cant be found then try and clear the pop up window if there and move on to next cin
        browser.find_element(By.CLASS_NAME, "rwCloseButton").click()
        time.sleep(1)
        return True
    except NoSuchElementException:
        return False


########################################################################################################################


def get_patient_language(browser):
    try:
        language = browser.find_element(By.ID, "ContentPlaceHolder1_ucSearchMember_lblLanguage").text
        time.sleep(1)
        return language
    except NoSuchElementException:
        return None


def get_special_messages(browser):
    try:
        special_message = browser.find_element(By.ID, "ContentPlaceHolder1_ucSearchMember_lblSpcMsgs").text
        time.sleep(1)
        return special_message
    except NoSuchElementException:
        return None


def click_search_new_member(browser):
    try:
        # click search new member and move to next cin in the list
        browser.find_element(By.ID, "ContentPlaceHolder1_btnSearchMember").click()
        return True
    except NoSuchElementException:
        return False
