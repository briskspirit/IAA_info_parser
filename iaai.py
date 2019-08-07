from bs4 import BeautifulSoup
from selenium import webdriver
from consolemenu import *
from consolemenu.items import *
from datetime import date
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing.dummy import current_process
import json
import requests
import pathlib
import os
import time
import logging

#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename='iaai.log', filemode='a')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class iaai(object):

    cars_db_filename = ''
    base_dir = "C:\\Users\\BS\\Downloads\\CARS\\"
    json_path = base_dir + "DB\\"
    screenshots_path = base_dir + "SCREENSHOTS\\"
    photos_path = base_dir + "PHOTOS\\"
    html_parse_path = base_dir + "HTML\\"
    html_bckp_path = base_dir + "old_html\\"
    geckodriver_path = base_dir + "bin\\geckodriver.exe"
    today_str = ''  # Just today's date in str format yyyy-mm-dd
    html_to_read = ''
    cars_db = {}
    enhance_car_limit = 10000
    autosave_period = 20
    driver = {}
    selenium_page_timeout = 10

    requests_socks = {
        'http': None,  # "socks5://127.0.0.1:3128"
        'https': None  # "socks5://127.0.0.1:3128"
    }
    selenium_socks = [None, 3128]  # ["127.0.0.1", 3128]
    selenium_profile = webdriver.FirefoxProfile()

    def __init__(self, db_file):

        self.today_str = str(date.today())
        self.cars_db_filename = db_file
        self.read_cars_db()
        log.info("Cars in loaded DB: " + str(len(self.cars_db.keys())))

        if self.selenium_socks[0] != None:
            self.selenium_profile.set_preference("network.proxy.type", 1)
            self.selenium_profile.set_preference("network.proxy.socks", self.selenium_socks[0])
            self.selenium_profile.set_preference("network.proxy.socks_port", self.selenium_socks[1])
            self.selenium_profile.set_preference("network.proxy.socks_version", 5)
            self.selenium_profile.set_preference("network.proxy.socks_remote_dns", True)

    def only_digits(self, data):
        return ''.join(c for c in data if c.isdigit())

    def read_text_file(self, filename):
        with open(filename, 'r') as file:
            return file.read()

    def write_cars_db(self):
        with open(self.json_path + self.cars_db_filename + ".json", 'w') as file:
            log.info("Cars in saved DB: " + str(len(self.cars_db.keys())))
            file.write(json.dumps(self.cars_db, sort_keys=True, indent=4))

    def download_file(self, url, save_path):
        if self.requests_socks['https'] == None:  # Check if socks is set
            archive = requests.get(url, stream=True).content
        else:
            archive = requests.get(url, proxies=self.requests_socks, stream=True).content
        with open(save_path, "wb") as file:
            file.write(archive)

    def read_cars_db(self):

        try:
            with open(self.json_path + self.cars_db_filename + ".json", 'r') as file:
                log.debug("Reading DB file...")
                try:
                    self.cars_db = json.loads(file.read())
                    log.debug("File was read, parsing JSON")
                except ValueError:
                    log.warning("Can't read DB file")
        except FileNotFoundError:
            pathlib.Path(self.json_path + self.cars_db_filename + ".json").touch()
            pathlib.Path(self.photos_path + self.cars_db_filename).mkdir(parents=True, exist_ok=True)
            pathlib.Path(self.screenshots_path + self.cars_db_filename).mkdir(parents=True, exist_ok=True)
            log.warning("No such DB file, will create file and directories")

    def parse_html_file(self, file):

        parsed_html = BeautifulSoup(self.read_text_file(file), features="lxml")
        table = parsed_html.body.find('div', attrs={'data-dojo-attach-point': 'apPreviousGrid'}).find('div', attrs={'class': 'dgrid-content ui-widget-content'})
        rows = table.find_all('div', attrs={'class': 'ui-state-default'})

        for row in rows:
            cells = row.find_all('div', attrs={'class': 'tableCell'})
            href = row.find('a', href=True)['href']
            itemid = href.split('itemid=')[1]

            if itemid not in self.cars_db:

                self.cars_db[itemid] = {}
                self.cars_db[itemid]['href'] = href
                self.cars_db[itemid]['year'] = self.only_digits(cells[1].text)
                self.cars_db[itemid]['make'] = cells[2].text.strip()
                self.cars_db[itemid]['model'] = cells[3].text.strip()
                self.cars_db[itemid]['damage'] = cells[4].text.strip()
                self.cars_db[itemid]['price'] = self.only_digits(cells[8].text)

            else:
                log.warning("We already have this stock #: " + itemid)

    def parse_car_properties(self, record, parsed_car):

        try:
            record['VIN'] = parsed_car.find('span', attrs={'class': 'VIN_vehicleStats'}).text.strip()
            record['Stock#'] = parsed_car.find('div', attrs={'class': 'pd-title-info pd-title-stock'}).text.split('#: ')[1].strip()
            infos = parsed_car.find_all('div', attrs={'class': 'col-7 col-value flex-self-end'})
            labels = parsed_car.find_all('div', attrs={'class': 'col-5 col-label'})
            for label, info in zip(labels, infos):
                record[label.text.strip().replace("\t", "").replace("  ", " ")] = info.text.strip().replace("\t", "").replace("  ", " ")

            images_url = "https://www.iaai.com" + parsed_car.find('a', attrs={'class': 'btn-icon-img btn-icon-download tooltipstered'}, href=True)['href'].strip()
            record['Images_URL'] = images_url

            record['parsed_car'] = "1"
            return record

        except AttributeError:
            log.warning("Can't parse car info(page not available?)")
            record['parsed_car'] = "-1"
            return record

    def enhance_car_db(self, itemid):

        thread_id = int(str(current_process()).split("Thread-")[1].split(", ")[0])
        log.info("Worker thread number " + str(thread_id))

        if thread_id not in self.driver:
            try:
                self.driver[thread_id] = webdriver.Firefox(executable_path=self.geckodriver_path, firefox_profile=self.selenium_profile)
                self.driver[thread_id].set_page_load_timeout(self.selenium_page_timeout)
                log.debug("Creating selenium driver for worker thread number " + str(thread_id))
            except Exception as error:
                log.error("Can't create selenium driver for worker thread number " + str(thread_id))
                log.error(str(error))

        i = 0
        autosave = 0

        if 'parsed_car' not in self.cars_db[itemid] and 'href' in self.cars_db[itemid]:
            log.debug("Parsing car: " + itemid)
            self.driver[thread_id].get(self.cars_db[itemid]['href'])
            self.driver[thread_id].get_screenshot_as_file(self.screenshots_path + self.cars_db_filename + "\\" + itemid + ".png")
            self.cars_db[itemid] = self.parse_car_properties(self.cars_db[itemid], BeautifulSoup(self.driver[thread_id].page_source, features="lxml"))
            i += 1
            autosave += 1
        elif 'href' not in self.cars_db[itemid]:
            log.debug("Missing url: " + itemid)
        else:
            log.debug("Already parsed: " + itemid)

        if 'Images_URL' in self.cars_db[itemid] and 'images_downloaded' not in self.cars_db[itemid]:
            self.download_file(self.cars_db[itemid]['Images_URL'], self.photos_path + self.cars_db_filename + "\\" + itemid + ".zip")
            self.cars_db[itemid]['images_downloaded'] = "1"
        else:
            log.debug("Photos already downloaded for: " + itemid)

        if autosave >= self.autosave_period:
            autosave = 0
            self.write_cars_db()

    def close_drivers(self):
        for key in self.driver.copy().keys():
            try:
                self.driver[key].close()
            except:
                pass

    def quit_drivers(self):
        for key in self.driver.copy().keys():
            try:
                self.driver[key].quit()
            except:
                pass

    def clean_nonenhanced(self):

        log.info("Cars in DB before cleaning: " + str(len(self.cars_db.keys())))
        for itemid in self.cars_db.copy().keys():
            if 'parsed_car' in self.cars_db[itemid] and self.cars_db[itemid]['parsed_car'] == "-1":
                self.cars_db.pop(itemid)
                if pathlib.Path(self.photos_path + self.cars_db_filename + "\\" + itemid + ".zip").is_file():
                    pathlib.Path(self.photos_path + self.cars_db_filename + "\\" + itemid + ".zip").unlink()
                if pathlib.Path(self.screenshots_path + self.cars_db_filename + "\\" + itemid + ".png").is_file():
                    pathlib.Path(self.screenshots_path + self.cars_db_filename + "\\" + itemid + ".png").unlink()
        log.info("Cars in DB after cleaning: " + str(len(self.cars_db.keys())))


def wait_for_operator():

    key = input()

    while True:
        if key == 'Y' or key == 'y':
            return True
            break
        elif key == 'N' or key == 'n':
            return False
            break
        else:
            log.error("Wrong key input! Y or N only!")
            key = input()


def parse_html_menu():
    print("Do you want to parse html to DB?")
    if wait_for_operator():
        if len(os.listdir(iaai.html_parse_path)) > 0:
            today_path = iaai.html_bckp_path + iaai.today_str
            pathlib.Path(today_path).mkdir(parents=True, exist_ok=True)
            for file in os.listdir(iaai.html_parse_path):
                if ".html" in file or ".htm" in file:
                    iaai.parse_html_file(iaai.html_parse_path + file)
                    iaai.write_cars_db()
                    os.rename(iaai.html_parse_path + file, today_path + "\\" + file)
        else:
            log.warning("No html files to parse!")
        print("Press any key...")
        input()


def enhance_cars_menu():
    print("Do you want to enhance cars in DB?(download more info and photos)")
    if wait_for_operator():
        while True:
            try:
                pool = ThreadPool(4)
                pool.map(iaai.enhance_car_db, iaai.cars_db)
            except Exception as error:
                iaai.close_drivers()
                log.error(str(error))
                log.info("Error. Destroing drivers and will try again in 5 seconds...")
                time.sleep(5)
            else:
                iaai.quit_drivers()
                print("Enhancment was finished successfuly. Press any key...")
                input()
                break
            finally:
                pool.close()
                pool.terminate()
                pool.join()
                iaai.write_cars_db()


def clean_cars_menu():
    print("Do you want to clean cars that couldn't be enhanced from DB?")
    if wait_for_operator():
        iaai.clean_nonenhanced()
        iaai.write_cars_db()
        print("Press any key...")
        input()


def list_attributes_menu():
    print("Enter attribute by which to find unique values. Enter 0 to exit")
    while True:
        att = input()
        att_list = {}
        no_att = 0
        if att == "0":
            break
        else:
            for key in iaai.cars_db.keys():
                if att in iaai.cars_db[key]:
                    if iaai.cars_db[key][att] not in att_list:
                        att_list[iaai.cars_db[key][att]] = 1
                    else:
                        att_list[iaai.cars_db[key][att]] += 1
                else:
                    no_att += 0

            print("Here is a list with unique attributes values:\n")
            sorted_att_list = sorted(att_list.items(), key=lambda x: x[1], reverse=True)
            for (key, value) in sorted_att_list:
                print(key, value)
            print("Cars with no such attribute:", no_att)


if __name__ == '__main__':

    # No need to use .json extension!
    db_file = str(date.today())
    #db_file = "2019-03-04"

    iaai = iaai(db_file)

    menu = ConsoleMenu("IAAI.com Auction Parser", "Cars in DB: " + str(len(iaai.cars_db)) + " DB file: " + db_file)
    parse_html_menu = FunctionItem("Parse html files from auctions", parse_html_menu)
    enhance_cars_menu = FunctionItem("Enhance cars in DB(download more info and photos)", enhance_cars_menu)
    clean_cars_menu = FunctionItem("Clean cars from DB that could't be enhanced", clean_cars_menu)
    list_attributes_menu = FunctionItem("Find unique attribute values", list_attributes_menu)

    menu.append_item(parse_html_menu)
    menu.append_item(enhance_cars_menu)
    menu.append_item(clean_cars_menu)
    menu.append_item(list_attributes_menu)
    menu.show()
