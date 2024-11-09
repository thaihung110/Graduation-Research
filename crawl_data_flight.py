import random
from time import sleep

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from selenium import webdriver  # type: ignore
from selenium.common.exceptions import (  # type: ignore
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.chrome.service import Service  # type: ignore
from selenium.webdriver.common.action_chains import ActionChains  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore

# Tạo Service object với đường dẫn tới chromedriver
service = Service(r"D:\GR1\code\chromedriver.exe")

# Khởi tạo Chrome WebDriver với Service object
driver = webdriver.Chrome(service=service)
driver.maximize_window()


# get url
url = "https://www.google.com/travel/flights/search?tfs=CBwQAhopEgoyMDI1LTAxLTAxag0IAxIJL20vMDJfMjg2cgwIAxIIL20vMDRqcGxAAUgBcAGCAQsI____________AZgBAg&tfu=EgYIAhAAGAA&hl=en-US&curr=USD"
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
print(ticket_class_text)


# get departure time
elems = driver.find_elements(By.CSS_SELECTOR, ".wtdjmc.YMlIz.ogfYpf.tPgKwe")
departure_times = [elem.get_attribute("aria-label") for elem in elems]
# Sử dụng list comprehension để trích xuất giờ, định dạng AM/PM và thêm dấu cách giữa giờ và AM/PM
extracted_departure_times = [
    time.split("Departure time: ")[1]
    .replace("\u202f", "")
    .strip(".")
    .replace("AM", " AM")
    .replace("PM", " PM")
    for time in departure_times
]
print(extracted_departure_times)
print(len(extracted_departure_times))


num_of_flights = len(extracted_departure_times)
print(num_of_flights)

# get day
input_element = driver.find_element(By.CLASS_NAME, "TP4Lpb")
# Lấy giá trị từ thuộc tính "value"
date_value = input_element.get_attribute("value")
print(date_value)

# Assuming date_value is in the format "Mon, Jan 1" and you want to convert it to "2025-01-01"
# First, remove the day of the week
date_parts = date_value.split(", ")[1]  # Extract "Jan 1"
month_day = date_parts.split(" ")  # Split into ["Jan", "1"]

# Map month names to numbers
month_map = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

# Format the date as "2025-mm-dd"
formatted_date = f"2025-{month_map[month_day[0]]}-{int(month_day[1]):02d}"

# Create an array with the formatted date repeated num_of_flights times
date_value_list = [formatted_date] * num_of_flights
print(date_value_list)
print(len(date_value_list))

# get arrival time
elems = driver.find_elements(By.CSS_SELECTOR, ".XWcVob.YMlIz.ogfYpf.tPgKwe")
arrival_times = [elem.get_attribute("aria-label") for elem in elems]
# Sử dụng list comprehension để trích xuất giờ, định dạng AM/PM và loại bỏ ký tự \u202f
extracted_arrival_times = [
    time.split("Arrival time: ")[1]
    .replace("\u202f", "")
    .strip(".")
    .replace("AM", " AM")
    .replace("PM", " PM")
    for time in arrival_times
]
print(extracted_arrival_times)
print(len(extracted_arrival_times))

# get airline
elems = driver.find_elements(By.CSS_SELECTOR, ".h1fkLb")
# Lấy hãng bay đầu tiên từ mỗi phần tử
airlines = [
    elem.get_attribute("innerText").split(",")[0].strip() for elem in elems
]
print(airlines)
print(len(airlines))

# get number of stops
elems = driver.find_elements(By.CSS_SELECTOR, ".VG3hNb")
number_of_stops = [elem.get_attribute("innerText") for elem in elems]
print(number_of_stops)
print(len(number_of_stops))

# Create isNonStop array
isNonStop = [1 if stops == "Nonstop" else 0 for stops in number_of_stops]
print(isNonStop)

# Tìm tất cả các thẻ <span> có thuộc tính 'aria-label' chứa giá tiền (Vietnamese dong)
elems = driver.find_elements(
    By.XPATH, "//span[@role='text' and contains(@aria-label, 'US dollars')]"
)

# Lấy và in tất cả giá tiền
totalFare = []
for elem in elems:
    price = elem.text
    if price != "":
        totalFare.append(price)

# Xóa dấu $ ở mỗi phần tử trong price_list
totalFare = [price.replace("$", "") for price in totalFare]

# increase the length of price_list
while len(totalFare) < len(extracted_departure_times):
    totalFare.append("Price Unavailable")

print(totalFare)
print(len(totalFare))


# get durations
elems = driver.find_elements(By.CSS_SELECTOR, ".gvkrdb.AdWm1c.tPgKwe.ogfYpf")
durations = [elem.get_attribute("innerText") for elem in elems]

# Convert durations to float hours
formatted_durations = []
for duration in durations:
    parts = duration.split()
    hours = int(parts[0])  # Extract hours
    minutes = (
        int(parts[2]) if len(parts) > 2 else 0
    )  # Extract minutes, if present
    total_hours = hours + minutes / 60.0  # Convert to total hours
    formatted_durations.append(
        f"{total_hours:.2f}"
    )  # Format to 2 decimal places

print(formatted_durations)
print(len(formatted_durations))


ticket_class_list = [ticket_class_text] * num_of_flights
date_value_list = [date_value] * num_of_flights
print(ticket_class_list)
print(len(ticket_class_list))

# Create isBasicEconomy array
isBasicEconomy = [
    1 if ticket_class == "Economy" else 0 for ticket_class in ticket_class_list
]
print(isBasicEconomy)


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

# Create isRefundable array with all elements as 0
isRefundable = [0] * num_of_flights
print(isRefundable)

import pandas as pd

# Assuming the following variables are already defined and populated:
# date_value_list, departure_airport, arrival_airport, isBasicEconomy, isRefundable, isNonStop,
# totalFare, airlines, formatted_durations, extracted_departure_times, extracted_arrival_times

# Create a DataFrame
df = pd.DataFrame(
    {
        "flightDate": date_value_list,
        "startingAirport": departure_airport,
        "destinationAirport": arrival_airport,
        "isBasicEconomy": isBasicEconomy,
        "isRefundable": isRefundable,
        "isNonStop": isNonStop,
        "totalFare": totalFare,
        "segmentsAirlineName": airlines,
        "travelDuration": formatted_durations,
        "departureTime": extracted_departure_times,
        "arrivalTime": extracted_arrival_times,
    }
)

# Add flightKey column with sequential numbers starting from 1
df["flightKey"] = range(1, len(df) + 1)

# Display the DataFrame
print(df)


# Assuming the DataFrame 'df' is already created as shown previously

# Remove rows where totalFare is 'Price Unavailable'
df = df[df["totalFare"] != "Price Unavailable"]

# Display the updated DataFrame
print(df)


# Assuming the DataFrame 'df' is already created as shown previously

# Export the DataFrame to a CSV file
df.to_csv(r"D:\GR1\dataset\price_crawl\flights_crawl.csv", index=False)

# Confirm the file has been saved
print("Data has been exported to flights_data.csv")
