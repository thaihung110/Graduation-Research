import random
from time import sleep

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Tạo Service object với đường dẫn tới chromedriver
service = Service(r"D:\GR1\code\chromedriver.exe")

# Khởi tạo Chrome WebDriver với Service object
driver = webdriver.Chrome(service=service)
driver.maximize_window()

# get url
url = "https://www.google.com/travel/flights/search?tfs=CBwQAhooEgoyMDI0LTExLTAxagwIAhIIL20vMGZuZmZyDAgDEggvbS8wNGpwbEABSAFwAYIBCwj___________8BmAEC&tfu=EgYIAhAAGAA&hl=en-US&curr=USD"
driver.get(url)
sleep(random.randint(3, 5))

# Sử dụng XPath với contains() để tìm các giá trị như '9 more flights', '10 more flights'
button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable(
        (By.XPATH, '//button[contains(@aria-label, "more flights")]')
    )
)

# Nhấn vào nút
action = ActionChains(driver)
action.move_to_element(button).click().perform()
sleep(10)


# get flight class
# Tìm thẻ span chứa hạng vé dựa trên id hoặc class
ticket_class = driver.find_element(
    By.XPATH,
    "/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div/div[1]/div[3]/div/div/div/div[1]/span[4]/span",
)

# Lấy nội dung text từ thẻ span
ticket_class_text = ticket_class.text
print(f"Ticket class is: {ticket_class_text}")


# get day
input_element = driver.find_element(By.CLASS_NAME, "TP4Lpb")
# Lấy giá trị từ thuộc tính "value"
date_value = input_element.get_attribute("value")
print(f"Day of flight is: {date_value}")

# get departure time
elems = driver.find_elements(By.CSS_SELECTOR, ".wtdjmc.YMlIz.ogfYpf.tPgKwe")
departure_times = [elem.get_attribute("aria-label") for elem in elems]
# Sử dụng list comprehension để trích xuất giờ, định dạng AM/PM và loại bỏ ký tự \u202f
extracted_departure_times = [
    time.split("Departure time: ")[1].replace("\u202f", "").strip(".")
    for time in departure_times
]
print(f"List of departure time: {extracted_departure_times}")


num_of_flights = len(extracted_departure_times)
print(f"Number of flights: {num_of_flights}")

# get arrival time
elems = driver.find_elements(By.CSS_SELECTOR, ".XWcVob.YMlIz.ogfYpf.tPgKwe")
arrival_times = [elem.get_attribute("aria-label") for elem in elems]
# Sử dụng list comprehension để trích xuất giờ, định dạng AM/PM và loại bỏ ký tự \u202f
extracted_arrival_times = [
    time.split("Arrival time: ")[1].replace("\u202f", "").strip(".")
    for time in arrival_times
]
print(f"List of arrival time: {extracted_arrival_times}")

# get airline
elems = driver.find_elements(By.CSS_SELECTOR, ".h1fkLb")
airlines = [elem.get_attribute("innerText") for elem in elems]
print(airlines)
print(len(airlines))


# get number of stops
elems = driver.find_elements(By.CSS_SELECTOR, ".VG3hNb")
number_of_stops = [elem.get_attribute("innerText") for elem in elems]
print(f"Number of stops: {number_of_stops}")


# Tìm tất cả các thẻ <span> có thuộc tính 'aria-label' chứa giá tiền (Vietnamese dong)
elems = driver.find_elements(
    By.XPATH, "//span[@role='text' and contains(@aria-label, 'US dollars')]"
)

# Lấy và in tất cả giá tiền
price_list = []
for elem in elems:
    price = elem.text
    if price != "":
        price_list.append(price)


# increase the length of price_list
while len(price_list) < len(extracted_departure_times):
    price_list.append("Price Unavailable")

print(f"List of price: {price_list}")

import time

from selenium.webdriver.common.action_chains import ActionChains

# Lặp qua từng chuyến bay
for i in range(1, num_of_flights + 1):
    # Tạo Xpath cho nút cần nhấn
    xpath = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[3]/div/div/button"

    try:

        # # Cuộn xuống để nút có thể hiển thị
        # WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        button = driver.find_element(By.XPATH, xpath)
        actions = ActionChains(driver)
        actions.move_to_element(button).perform()

        # Nhấn nút
        button.click()
        print(f"Đã nhấn nút chuyến bay {i}")

        time.sleep(1)  # Thời gian chờ giữa các lần nhấn

    except Exception as e:
        print(f"Không thể nhấn nút chuyến bay {i}: {e}")

departure_airports = [""] * num_of_flights
arrival_airports = [""] * num_of_flights

for i in range(1, num_of_flights + 1):
    if number_of_stops[i - 1] == "Nonstop":
        path_departure_airport = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[3]"
        path_arrival_airport = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[3]"

        departure_airport = driver.find_element(
            By.XPATH, path_departure_airport
        ).text.strip("()")
        arrival_airport = driver.find_element(
            By.XPATH, path_arrival_airport
        ).text.strip("()")

        departure_airports[i - 1] = departure_airport
        arrival_airports[i - 1] = arrival_airport

    elif number_of_stops[i - 1] == "1 stop":
        path_departure_airport_one = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[3]"
        path_arrival_airport_one = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[3]"
        path_departure_airport_two = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[3]/span[3]"
        path_arrival_airport_two = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[5]/span[3]"

        departure_airport_one = driver.find_element(
            By.XPATH, path_departure_airport_one
        ).text.strip("()")
        arrival_airport_one = driver.find_element(
            By.XPATH, path_arrival_airport_one
        ).text.strip("()")
        departure_airport_two = driver.find_element(
            By.XPATH, path_departure_airport_two
        ).text.strip("()")
        arrival_airport_two = driver.find_element(
            By.XPATH, path_arrival_airport_two
        ).text.strip("()")

        departure_airport_concat = (
            departure_airport_one + "||" + departure_airport_two
        )
        arrival_airport_concat = (
            arrival_airport_one + "||" + arrival_airport_two
        )
        departure_airports[i - 1] = departure_airport_concat
        arrival_airports[i - 1] = arrival_airport_concat

    elif number_of_stops[i - 1] == "2 stops":
        path_departure_airport_one = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[3]"
        path_arrival_airport_one = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[3]"
        path_departure_airport_two = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[3]/span[3]"
        path_arrival_airport_two = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[5]/span[3]"
        path_departure_airport_three = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[3]/div[3]/span[3]"
        path_arrival_airport_three = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[3]/div[5]/span[3]"

        departure_airport_one = driver.find_element(
            By.XPATH, path_departure_airport_one
        ).text.strip("()")
        arrival_airport_one = driver.find_element(
            By.XPATH, path_arrival_airport_one
        ).text.strip("()")
        departure_airport_two = driver.find_element(
            By.XPATH, path_departure_airport_two
        ).text.strip("()")
        arrival_airport_two = driver.find_element(
            By.XPATH, path_arrival_airport_two
        ).text.strip("()")
        departure_airport_three = driver.find_element(
            By.XPATH, path_departure_airport_three
        ).text.strip("()")
        arrival_airport_three = driver.find_element(
            By.XPATH, path_arrival_airport_three
        ).text.strip("()")

        departure_airport_concat = (
            departure_airport_one
            + "||"
            + departure_airport_two
            + "||"
            + departure_airport_three
        )
        arrival_airport_concat = (
            arrival_airport_one
            + "||"
            + arrival_airport_two
            + "||"
            + arrival_airport_three
        )
        departure_airports[i - 1] = departure_airport_concat
        arrival_airports[i - 1] = arrival_airport_concat

    print(f"Complete airport for flight {i}")

departure_timepoints = [""] * num_of_flights
arrival_timepoints = [""] * num_of_flights

for i in range(1, num_of_flights + 1):
    if number_of_stops[i - 1] == "Nonstop":
        departure_timepoint_path = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[1]/span/span/span"
        arrival_timepoint_path = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[1]/span/span/span"

        departure_timepoint = driver.find_element(
            By.XPATH, departure_timepoint_path
        ).text
        arrival_timepoint = driver.find_element(
            By.XPATH, arrival_timepoint_path
        ).text

        departure_timepoints[i - 1] = departure_timepoint
        arrival_timepoints[i - 1] = arrival_timepoint
        print(departure_timepoints[i - 1])
        print(arrival_timepoints[i - 1])

    elif number_of_stops[i - 1] == "1 stop":
        departure_timepoint_path_1 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[1]/span/span/span"
        arrival_timepoint_path_1 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[1]/span/span/span"
        departure_timepoint_path_2 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[3]/span[1]/span/span/span"
        arrival_timepoint_path_2 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[5]/span[1]/span/span/span"

        departure_timepoint_1 = driver.find_element(
            By.XPATH, departure_timepoint_path_1
        ).text
        arrival_timepoint_1 = driver.find_element(
            By.XPATH, arrival_timepoint_path_1
        ).text
        departure_timepoint_2 = driver.find_element(
            By.XPATH, departure_timepoint_path_2
        ).text
        arrival_timepoint_2 = driver.find_element(
            By.XPATH, arrival_timepoint_path_2
        ).text

        departure_timepoint_concat = (
            departure_timepoint_1 + " || " + departure_timepoint_2
        )
        arrival_timepoint_concat = (
            arrival_timepoint_1 + " || " + arrival_timepoint_2
        )

        departure_timepoints[i - 1] = departure_timepoint_concat
        arrival_timepoints[i - 1] = arrival_timepoint_concat
        print(departure_timepoints[i - 1])
        print(arrival_timepoints[i - 1])

    elif number_of_stops[i - 1] == "2 stops":
        departure_timepoint_path_1 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[4]/span[1]/span/span/span"
        arrival_timepoint_path_1 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[1]/div[6]/span[1]/span/span/span"
        departure_timepoint_path_2 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[3]/span[1]/span/span/span"
        arrival_timepoint_path_2 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[2]/div[5]/span[1]/span/span/span"
        departure_timepoint_path_3 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[3]/div[3]/span[1]/span/span/span"
        arrival_timepoint_path_3 = f"/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[2]/div[4]/ul/li[{i}]/div/div[4]/div/div[3]/div[5]/span[1]/span/span/span"

        departure_timepoint_1 = driver.find_element(
            By.XPATH, departure_timepoint_path_1
        ).text
        arrival_timepoint_1 = driver.find_element(
            By.XPATH, arrival_timepoint_path_1
        ).text
        departure_timepoint_2 = driver.find_element(
            By.XPATH, departure_timepoint_path_2
        ).text
        arrival_timepoint_2 = driver.find_element(
            By.XPATH, arrival_timepoint_path_2
        ).text
        departure_timepoint_3 = driver.find_element(
            By.XPATH, departure_timepoint_path_3
        ).text
        arrival_timeoint_3 = driver.find_element(
            By.XPATH, arrival_timepoint_path_3
        ).text

        departure_timepoint_concat = (
            departure_timepoint_1
            + " || "
            + departure_timepoint_2
            + " || "
            + departure_timepoint_3
        )
        arrival_timepoint_concat = (
            arrival_timepoint_1
            + " || "
            + arrival_timepoint_2
            + " || "
            + departure_timepoint_3
        )

        departure_timepoints[i - 1] = departure_timepoint_concat
        arrival_timepoints[i - 1] = arrival_timepoint_concat
        print(departure_timepoints[i - 1])
        print(arrival_timepoints[i - 1])

    print(f"complete time for flight {i}")

# get durations
elems = driver.find_elements(By.CSS_SELECTOR, ".gvkrdb.AdWm1c.tPgKwe.ogfYpf")
durations = [elem.get_attribute("innerText") for elem in elems]
print(f"List of durations: {durations}")

ticket_class_list = [ticket_class_text] * num_of_flights
date_value_list = [date_value] * num_of_flights

# get departure airport
elems = driver.find_elements(By.CSS_SELECTOR, ".G2WY5c.sSHqwe.ogfYpf.tPgKwe")
departure_airport = [elem.get_attribute("innerText") for elem in elems]
print(departure_airport)
print(len(departure_airport))

# get arrival airport
elems = driver.find_elements(By.CSS_SELECTOR, ".c8rWCd.sSHqwe.ogfYpf.tPgKwe")
arrival_airport = [elem.get_attribute("innerText") for elem in elems]
print(arrival_airport)
print(len(arrival_airport))

df = pd.DataFrame(
    list(
        zip(
            date_value_list,
            extracted_departure_times,
            extracted_arrival_times,
            departure_airport,
            arrival_airport,
            airlines,
            number_of_stops,
            departure_airports,
            arrival_airports,
            departure_timepoints,
            arrival_timepoints,
            price_list,
            durations,
            ticket_class_list,
        )
    ),
    columns=[
        "Departure Date",
        "Departure Time",
        "Arrival Time",
        "Starting Airport",
        "Destination Airport",
        "Airline",
        "Number_of_stops",
        "List of Departure Airport",
        "List of Arrival Airport",
        "List of Departure Timepoint",
        "List of Arrival Timepoint",
        "Price",
        "Duration",
        "Ticket Class",
    ],
)

df.to_csv("flight_data.csv", index=False)

print(df)
