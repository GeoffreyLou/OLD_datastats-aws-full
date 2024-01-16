from selenium import webdriver
from bs4 import BeautifulSoup

options = webdriver.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)

print('Trying to get website')

driver.get("https://books.toscrape.com/")

print('We have the website !')

soup = BeautifulSoup(driver.page_source, "html.parser")

print('The soup is : ', soup)

menu_element = soup.find_all('li', {'class': 'col-xs-6 col-sm-4 col-md-3 col-lg-3'})

print('The menu_element is : ', menu_element)

for i in menu_element:
    print(i.find('h3').find("a").text)

print('Success')
