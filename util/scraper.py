from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from time import sleep
import pandas as pd
from util.processor import fill_historical_values, fill_forecast_values

TODAY_DATETIME = pd.to_datetime(pd.to_datetime('today').date())

HISTORY_XPATH = "/html/body/app-root/app-history/one-column-layout/wu-header/sidenav/mat-sidenav-container" + \
                "/mat-sidenav-content/div/section/div[2]/div/div[5]/div/div/" + \
                "lib-city-history-observation/div/div[2]/table"

FORECAST_XPATH = "/html/body/app-root/app-hourly/one-column-layout/wu-header/sidenav/mat-sidenav-container" + \
                 "/mat-sidenav-content/div/section/div[3]/div[1]/div/div[1]/div/lib-city-hourly-forecast/div" + \
                 "/div[4]/div[1]/table"

WEATHER_SOURCES = {
    "ankara": "LTAC",
    "adana": "tr/seyhan/LTAF",
    "istanbul": "tr/pendik/LTFJ",
    "gaziantep": "tr/oguzeli/LTAJ"
}

HIST_HOUR_CONV_DICT = {
    "12:50 AM": 1, "6:50 AM": 7, "12:50 PM": 13, "6:50 PM": 19,
    "1:50 AM": 2, "7:50 AM": 8, "1:50 PM": 14, "7:50 PM": 20,
    "2:50 AM": 3, "8:50 AM": 9, "2:50 PM": 15, "8:50 PM": 21,
    "3:50 AM": 4, "9:50 AM": 10, "3:50 PM": 16, "9:50 PM": 22,
    "4:50 AM": 5, "10:50 AM": 11, "4:50 PM": 17, "10:50 PM": 23,
    "5:50 AM": 6, "11:50 AM": 12, "5:50 PM": 18, "11:50 PM": 24
}

FORECAST_HOUR_CONV_DICT = {
    "12:00 am": 0, "6:00 am": 6, "12:00 pm": 12, "6:00 pm": 18,
    "1:00 am": 1, "7:00 am": 7, "1:00 pm": 13, "7:00 pm": 19,
    "2:00 am": 2, "8:00 am": 8, "2:00 pm": 14, "8:00 pm": 20,
    "3:00 am": 3, "9:00 am": 9, "3:00 pm": 15, "9:00 pm": 21,
    "4:00 am": 4, "10:00 am": 10, "4:00 pm": 16, "10:00 pm": 22,
    "5:00 am": 5, "11:00 am": 11, "5:00 pm": 17, "11:00 pm": 23,
}

FORECAST_COLUMN_CONV_DICT = {
    "Temp.": "Temperature",
    "Amount": "Precip.",
    "Conditions": "Condition"
}

TABLE_COLUMNS = [
    'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind',
    'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.', 'Condition'
]


def create_new_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument('--log-level=3')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.wunderground.com/")
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings"]""")[0]
    celcius_button.click()
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings-quick"]/div/a[2]""")[0]
    celcius_button.click()
    return driver


def format_table(table, date, forecast=False):
    if not forecast:
        df = table.copy()
        df.replace(u"\xa0", u" ", regex=True, inplace=True)
        df = df[df["Time"].str.split(":", expand=True)[1].apply(lambda x: x[:2]) == "50"]
        for df_col in df.columns[1:-1]:
            df.loc[:, df_col] = df.loc[:, df_col].str.split(" ", expand=True)[0]
        df.Time = df.Time.replace(HIST_HOUR_CONV_DICT)
        df.insert(0, "WeatherDate", date)
        df.insert(0, "RecordDate", TODAY_DATETIME)
    else:
        df = table.copy()
        df.replace(u"\xa0", u" ", regex=True, inplace=True)
        df.Time = df.Time.replace(FORECAST_HOUR_CONV_DICT)
        df["Wind Speed"] = df.Wind.str.split(" ", expand=True)[0]
        df["Wind"] = df.Wind.str.split(" ", expand=True)[2]
        df["Wind Gust"] = 0
        df.insert(0, "WeatherDate", date)
        df.insert(0, "RecordDate", TODAY_DATETIME)
        df = df.rename(columns=FORECAST_COLUMN_CONV_DICT)[TABLE_COLUMNS]
        for df_col in df.columns[3:-2]:
            df.loc[:, df_col] = df.loc[:, df_col].astype(str).str.split(" ", expand=True)[0]
    return df


def convert_historical_table(df, city, date):
    df = df.copy()
    df.replace(u"\xa0", u" ", regex=True, inplace=True)
    df = df[df["Time"].str.split(":", expand=True)[1].apply(lambda x: x[:2]) == "50"]
    for df_col in df.columns[1:-1]:
        df.loc[:, df_col] = df.loc[:, df_col].str.split(" ", expand=True)[0]
    df.Time = df.Time.replace(HIST_HOUR_CONV_DICT)
    df.insert(0, "WeatherDate", date)
    df.insert(0, "RecordDate", TODAY_DATETIME)
    return df


def convert_forecast_table(df, city, date):
    df = df.copy()
    df.replace(u"\xa0", u" ", regex=True, inplace=True)
    df.Time = df.Time.replace(FORECAST_HOUR_CONV_DICT)
    df["Wind Speed"] = df.Wind.str.split(" ", expand=True)[0]
    df["Wind"] = df.Wind.str.split(" ", expand=True)[2]
    df["Wind Gust"] = 0
    df.insert(0, "WeatherDate", date)
    df.insert(0, "RecordDate", pd.to_datetime(pd.to_datetime("today").date()))
    df = df.rename(columns=FORECAST_COLUMN_CONV_DICT)[TABLE_COLUMNS]
    for df_col in df.columns[3:-2]:
        df.loc[:, df_col] = df.loc[:, df_col].astype(str).str.split(" ", expand=True)[0]
    return df


def update_historical_data(city, db_conn):
    start_date = pd.to_datetime(
        pd.read_sql_query(f"SELECT MAX(WeatherDate) FROM h_{city}", db_conn).values[0][0].split(" ")[0],
        format="%Y-%m-%d"
    )
    if start_date >= TODAY_DATETIME:
        print(f"The archive for city {city.capitalize()} is already up to date. Moving on.\n", end="")
        return None
    driver = create_new_driver(headless=True)
    new_data = []
    for day_cnt in range(int((TODAY_DATETIME - start_date) / pd.to_timedelta("1 days"))):
        curr_dt = start_date + pd.to_timedelta(f"{day_cnt} days")
        curr_url = f'https://www.wunderground.com/history/daily/{WEATHER_SOURCES[city]}/' \
                   f'date/{curr_dt.strftime("%Y-%m-%d")}'
        driver.get(curr_url)
        in_while = True
        while in_while:
            try:
                curr_table = WebDriverWait(driver, 100).until(
                    EC.presence_of_all_elements_located((By.XPATH, HISTORY_XPATH))
                )[0]
                in_while = False
            except Exception as e:
                print('There was an error, will try in 3 seconds.\n', end="")
                sleep(3)
        new_data.append(
            format_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                         curr_dt))
        print(f"HISTORICAL DATA  ::::  {city.capitalize().center(10)}  ::::  {curr_dt.date()}")
    new_df = pd.concat(new_data, axis=0, ignore_index=True)
    new_df = fill_historical_values(new_df)
    new_df.to_sql(f"h_{city}", db_conn, if_exists='append', index=False)
    driver.close()


def update_forecast_data(city, db_conn, days):
    last_date = pd.to_datetime(
        pd.read_sql_query(f"SELECT MAX(RecordDate) FROM f_{city}", db_conn).values[0][0].split(" ")[0],
        format="%Y-%m-%d"
    )
    if last_date >= TODAY_DATETIME:
        print(f'The archive for city {city.capitalize()} is already up to date. Moving on.\n', end="")
        return None
    end_date = TODAY_DATETIME + pd.to_timedelta(f"{days} days")

    driver = create_new_driver(headless=True)

    curr_dt = TODAY_DATETIME
    curr_url = f'https://www.wunderground.com/history/daily/{WEATHER_SOURCES[city]}/' \
               f'date/{curr_dt.strftime("%Y-%m-%d")}'
    driver.get(curr_url)
    new_data = []
    in_while = True
    while in_while:
        try:
            curr_table = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, HISTORY_XPATH))
            )[0]
            in_while = False
        except Exception as e:
            print(e)
            print('Encountered an error, moving on in 3.\n', end="")
            sleep(3)
    current_day_data = format_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                                    curr_dt, forecast=False)
    new_data.append(current_day_data)
    for day_cnt in range(int((end_date - TODAY_DATETIME) / pd.to_timedelta("1 days"))):
        curr_dt = TODAY_DATETIME + pd.to_timedelta(f"{day_cnt} days")
        curr_url = f'https://www.wunderground.com/hourly/{WEATHER_SOURCES[city]}/' \
                   f'date/{curr_dt.strftime("%Y-%m-%d")}'
        driver.get(curr_url)
        in_while = True
        while in_while:
            try:
                curr_table = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, FORECAST_XPATH))
                )[0]
                in_while = False
            except Exception as e:
                print(e)
                print("Encountered an error, moving on in 3.\n", end="")
                sleep(3)
        new_data.append(format_table(pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any"),
                                     curr_dt, forecast=True))
        print(f"FORECAST DATA  ::::  {city.capitalize().center(10)}  ::::  {curr_dt.date()}")
    new_df = pd.concat(new_data, axis=0, ignore_index=True)
    new_df = fill_forecast_values(new_df)
    new_df.to_sql(f"f_{city}", db_conn, if_exists='append', index=False)
    driver.close()
