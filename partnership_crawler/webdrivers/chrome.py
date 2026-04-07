from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


def browser(download_directory, headless):
    """
    :param download_directory: directory to download any files
    :param headless: True if the browser should open in the background, False
    :return: selenium.webdriver
    :resources:
        https://stackoverflow.com/questions/52049929/how-to-change-the-download-location-using-python-and-selenium-webdriver
        https://stackoverflow.com/questions/23381324/how-can-i-control-chromedriver-open-window-size
    """

    # set headless and window size options for Chrome
    chrome_options = ChromeOptions()
    # chrome_options.headless = headless
    if headless is True:
        chrome_options.add_argument('--headless=new')

    chrome_options.add_argument('--window-size=1920,1080')

    # set download directory for Chrome
    preferences = {
        'download.default_directory': download_directory,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True
    }
    chrome_options.add_experimental_option('prefs', preferences)

    # open up Chrome browser with adjusted settings
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
