from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from time import sleep
import pandas as pd
from util.processor import fill_historical_values, fill_forecast_values

_WUT_CONV_DICT = {
    "12:50 AM": 1, "6:50 AM": 7, "12:50 PM": 13, "6:50 PM": 19,
    "1:50 AM": 2, "7:50 AM": 8, "1:50 PM": 14, "7:50 PM": 20,
    "2:50 AM": 3, "8:50 AM": 9, "2:50 PM": 15, "8:50 PM": 21,
    "3:50 AM": 4, "9:50 AM": 10, "3:50 PM": 16, "9:50 PM": 22,
    "4:50 AM": 5, "10:50 AM": 11, "4:50 PM": 17, "10:50 PM": 23,
    "5:50 AM": 6, "11:50 AM": 12, "5:50 PM": 18, "11:50 PM": 24
}

_WUT_FORECAST_CONV_DICT = {
    "12:00 am": 0, "6:00 am": 6, "12:00 pm": 12, "6:00 pm": 18,
    "1:00 am": 1, "7:00 am": 7, "1:00 pm": 13, "7:00 pm": 19,
    "2:00 am": 2, "8:00 am": 8, "2:00 pm": 14, "8:00 pm": 20,
    "3:00 am": 3, "9:00 am": 9, "3:00 pm": 15, "9:00 pm": 21,
    "4:00 am": 4, "10:00 am": 10, "4:00 pm": 16, "10:00 pm": 22,
    "5:00 am": 5, "11:00 am": 11, "5:00 pm": 17, "11:00 pm": 23,
}

_FORECAST_COLUMN_CONV_DICT = {
    "Temp.": "Temperature",
    "Amount": "Precip.",
    "Conditions": "Condition"
}


# weather_data = {x: pd.DataFrame(columns=[
#     'City', 'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind',
#     'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.', 'Condition', 'IsForecast'
# ]) for x in _WEATHER_SOURCES.keys()}

_TABLE_COLUMNS = [
    'City', 'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind',
    'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.', 'Condition', 'IsForecast'
]


def convert_historical_table(df, city, date):
    df = df.copy()
    df.replace(u"\xa0", u" ", regex=True, inplace=True)
    df = df[df["Time"].str.split(":", expand=True)[1].apply(lambda x: x[:2]) == "50"]
    for df_col in df.columns[1:-1]:
        df.loc[:, df_col] = df.loc[:, df_col].str.split(" ", expand=True)[0]
    df.Time = df.Time.replace(_WUT_CONV_DICT)
    df.insert(0, "WeatherDate", date)
    df.insert(0, "RecordDate", pd.to_datetime(pd.to_datetime("today").date()))
    df.insert(0, "City", city.capitalize())
    df.insert(df.shape[1], "IsForecast", 0)
    return df


def convert_forecast_table(df, city, date):
    df = df.copy()
    df.replace(u"\xa0", u" ", regex=True, inplace=True)
    df.Time = df.Time.replace(_WUT_FORECAST_CONV_DICT)
    df["Wind Speed"] = df.Wind.str.split(" ", expand=True)[0]
    df["Wind"] = df.Wind.str.split(" ", expand=True)[2]
    df["Wind Gust"] = 0
    df.insert(0, "WeatherDate", date)
    df.insert(0, "RecordDate", pd.to_datetime(pd.to_datetime("today").date()))
    df.insert(0, "City", city.capitalize())
    df.insert(df.shape[1], "IsForecast", 1)
    df = df.rename(columns=_FORECAST_COLUMN_CONV_DICT)[_TABLE_COLUMNS]
    for df_col in df.columns[3:-2]:
        df.loc[:, df_col] = df.loc[:, df_col].astype(str).str.split(" ", expand=True)[0]
    return df


def update_historical_data(_current_archive, _wunderground_extensions, start_date):
    # noinspection PyTypeChecker
    _END_DATE = pd.to_datetime(pd.to_datetime("today").date())

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.wunderground.com/")
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings"]""")[0]
    celcius_button.click()
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings-quick"]/div/a[2]""")[0]
    celcius_button.click()

    del celcius_button

    for curr_city in _wunderground_extensions.keys():
        for day_cnt in range(int((_END_DATE - start_date) / pd.to_timedelta("1 days"))):
            curr_dt = start_date + pd.to_timedelta(f"{day_cnt} days")
            if (curr_dt in _current_archive[curr_city].WeatherDate.values) & \
                    (_current_archive[curr_city][_current_archive[curr_city].WeatherDate.eq(curr_dt)].shape[0] != 1):
                pass
            else:
                curr_url = f'https://www.wunderground.com/history/daily/{_wunderground_extensions[curr_city]}/' \
                           f'date/{curr_dt.year}-{str(curr_dt.month).zfill(2)}-{str(curr_dt.day).zfill(2)}'
                driver.get(curr_url)
                in_while = True
                while in_while:
                    try:
                        curr_table = WebDriverWait(driver, 20).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                        )[1]
                        in_while = False
                    except IndexError:
                        curr_table = WebDriverWait(driver, 20).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                        )[0]
                        if 'Condition' in pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(
                                how="any").columns:
                            in_while = False
                        else:
                            sleep(3)
                _current_archive[curr_city] = pd.concat([
                    _current_archive[curr_city],
                    convert_historical_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                                             curr_city, curr_dt)
                ], axis=0)
            print(f"{curr_city.capitalize()} - {curr_dt.date()}")
        _current_archive[curr_city] = fill_historical_values(_current_archive[curr_city])
        _current_archive[curr_city].reset_index(drop=True, inplace=True)
    driver.close()
    return _current_archive


def update_forecast_data(_current_archive, _wunderground_extensions):
    _START_DATE = pd.to_datetime(pd.to_datetime("today").date())
    # noinspection PyTypeChecker
    _END_DATE = pd.to_datetime(pd.to_datetime("today").date()) + pd.to_timedelta("15 days")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.wunderground.com/")
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings"]""")[0]
    celcius_button.click()
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings-quick"]/div/a[2]""")[0]
    celcius_button.click()

    del celcius_button

    for curr_city in _wunderground_extensions.keys():
        curr_dt = _START_DATE
        curr_url = f'https://www.wunderground.com/history/daily/{_wunderground_extensions[curr_city]}/' \
                   f'date/{curr_dt.year}-{str(curr_dt.month).zfill(2)}-{str(curr_dt.day).zfill(2)}'
        driver.get(curr_url)
        in_while = True
        while in_while:
            try:
                curr_table = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                )[1]
                in_while = False
            except IndexError:
                curr_table = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                )[0]
                if 'Condition' in pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(
                        how="any").columns:
                    in_while = False
                else:
                    sleep(3)
        current_day_data = convert_historical_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                         curr_city, curr_dt)
        current_day_data["IsForecast"] = 1
        _current_archive[curr_city] = pd.concat([
            _current_archive[curr_city],
            current_day_data
        ], axis=0)
        for day_cnt in range(int((_END_DATE - _START_DATE) / pd.to_timedelta("1 days"))):
            curr_dt = _START_DATE + pd.to_timedelta(f"{day_cnt} days")
            curr_url = f'https://www.wunderground.com/hourly/{_wunderground_extensions[curr_city]}/' \
                       f'date/{curr_dt.year}-{str(curr_dt.month).zfill(2)}-{str(curr_dt.day).zfill(2)}'
            driver.get(curr_url)
            in_while = True
            while in_while:
                try:
                    curr_table = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                    )[1]
                    in_while = False
                except IndexError:
                    curr_table = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table"))
                    )[0]
                    if 'Conditions' in pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(
                            how="any").columns:
                        in_while = False
                    else:
                        sleep(3)
            _current_archive[curr_city] = pd.concat([
                _current_archive[curr_city],
                convert_forecast_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                                         curr_city, curr_dt)
            ], axis=0)
            print(f"{curr_city.capitalize()} - {curr_dt.date()}")
        _current_archive[curr_city] = fill_forecast_values(_current_archive[curr_city])
        _current_archive[curr_city].reset_index(drop=True, inplace=True)
    driver.close()
    return _current_archive
