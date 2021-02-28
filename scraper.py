from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from time import sleep

_START_DATE = pd.to_datetime("20190101", format="%Y%m%d")
# noinspection PyTypeChecker
_END_DATE = pd.to_datetime(pd.to_datetime("today").date())


_WUT_CONV_DICT = {
    "12:50 AM": 1, "6:50 AM": 7, "12:50 PM": 13, "6:50 PM": 19,
    "1:50 AM": 2, "7:50 AM": 8, "1:50 PM": 14, "7:50 PM": 20,
    "2:50 AM": 3, "8:50 AM": 9, "2:50 PM": 15, "8:50 PM": 21,
    "3:50 AM": 4, "9:50 AM": 10, "3:50 PM": 16, "9:50 PM": 22,
    "4:50 AM": 5, "10:50 AM": 11, "4:50 PM": 17, "10:50 PM": 23,
    "5:50 AM": 6, "11:50 AM": 12, "5:50 PM": 18, "11:50 PM": 24
}


def convert_table(df, city, date):
    df = df.copy()
    df.replace(u"\xa0", u" ", regex=True, inplace=True)
    df = df[df["Time"].str.split(":", expand=True)[1].apply(lambda x: x[:2]) == "50"]
    for df_col in df.columns[1:-1]:
        df.loc[:, df_col] = df.loc[:, df_col].str.split(" ", expand=True)[0]
    df.Time = df.Time.replace(_WUT_CONV_DICT)
    df.insert(0, "WeatherDate", date)
    df.insert(0, "RecordDate", pd.to_datetime(pd.to_datetime("today").date()))
    df.insert(0, "City", city.capitalize())
    return df


# Conversion from °F to °C
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://www.wunderground.com/")
celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings"]""")[0]
celcius_button.click()
celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings-quick"]/div/a[2]""")[0]
celcius_button.click()

del celcius_button

# weather_data = {x: pd.DataFrame(columns=[
#     'City', 'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind',
#     'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.', 'Condition', 'IsForecast'
# ]) for x in _WEATHER_SOURCES.keys()}

weather_data = {x: pd.read_pickle(f"{x.lower()}.bak") for x in _WEATHER_SOURCES.keys()}

for curr_city in _WEATHER_SOURCES.keys():
    if curr_city == "ISKENDERUN":
        continue
    else:
        for day_cnt in range(int((_END_DATE - _START_DATE) / pd.to_timedelta("1 days"))):
            curr_dt = _START_DATE + pd.to_timedelta(f"{day_cnt} days")
            if curr_dt in weather_data[curr_city].WeatherDate.values:
                pass
            else:
                curr_url = f'https://www.wunderground.com/history/daily/{_WEATHER_SOURCES[curr_city]}/' \
                           f'date/{curr_dt.year}-{str(curr_dt.month).zfill(2)}-{str(curr_dt.day).zfill(2)}'
                driver.get(curr_url)
                in_while = True
                while in_while:
                    try:
                        curr_table = WebDriverWait(driver, 100).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                        )[1]
                        in_while = False
                    except IndexError:
                        curr_table = WebDriverWait(driver, 100).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                        )[0]
                        if 'Condition' in pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any").columns:
                            in_while = False
                        else:
                            sleep(3)
                weather_data[curr_city] = pd.concat([
                    weather_data[curr_city],
                    convert_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"), curr_city, curr_dt)
                ], axis=0)
            print(f"{curr_city.capitalize()} - {curr_dt.date()}")

for curr_city in _WEATHER_SOURCES.keys():
    weather_data[curr_city].to_pickle(f"{curr_city.lower()}.bak")

    for curr_city in _WEATHER_SOURCES.keys():
        weather_data[curr_city].insert(13, "IsForecast", 0)
