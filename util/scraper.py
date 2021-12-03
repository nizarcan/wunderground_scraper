from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from time import sleep
import pandas as pd
from util.processor import fill_historical_values, fill_forecast_values
import config as config
import constants as constants


def start_date_control(db_conn, city, start_date):
    try:
        new_start_date = pd.to_datetime(
            pd.read_sql_query(f"SELECT MAX(WeatherDate) FROM h_{city}", db_conn).values[0][0].split(" ")[0],
            format="%Y-%m-%d"
        )
    except Exception as e:
        new_start_date = pd.to_datetime(start_date)
    return new_start_date


def get_table(driver, url, xpath, curr_dt):
    driver.get(url)
    in_while = True
    while in_while:
        try:
            curr_table = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath))
            )[0]
            in_while = False
        except Exception as e:
            print(e)
            print('Encountered an error, moving on in 3.\n', end="")
            sleep(3)
    return pd.read_html(curr_table.get_attribute('outerHTML'))[0].dropna(how="any")


def format_table(table, date, forecast=False):
    if not forecast:
        df = table.copy()
        df.replace(u"\xa0", u" ", regex=True, inplace=True)
        df = df[df["Time"].str.split(":", expand=True)[1].apply(lambda x: x[:2]) == "50"]
        for df_col in df.columns[1:-1]:
            df.loc[:, df_col] = df.loc[:, df_col].str.split(" ", expand=True)[0]
        df.Time = df.Time.replace(constants.HIST_HOUR_CONV_DICT)
        df.insert(0, "WeatherDate", date)
        df.insert(0, "RecordDate", constants.TODAY_DATETIME)
    else:
        df = table.copy()
        df.replace(u"\xa0", u" ", regex=True, inplace=True)
        df.Time = df.Time.replace(constants.FORECAST_HOUR_CONV_DICT)
        df["Wind Speed"] = df.Wind.str.split(" ", expand=True)[0]
        df["Wind"] = df.Wind.str.split(" ", expand=True)[2]
        df["Wind Gust"] = 0
        df.insert(0, "WeatherDate", date)
        df.insert(0, "RecordDate", constants.TODAY_DATETIME)
        df = df.rename(columns=constants.FORECAST_COLUMN_CONV_DICT)[constants.TABLE_COLUMNS]
        for df_col in df.columns[3:-2]:
            df.loc[:, df_col] = df.loc[:, df_col].astype(str).str.split(" ", expand=True)[0]
    return df


def update_historical_data(driver, city, db_conn=None, sd="2018-01-01"):
    start_date = start_date_control(db_conn, city, sd)

    if start_date >= constants.TODAY_DATETIME:
        print(f"The archive for city {city.capitalize()} is already up to date. Moving on.\n", end="")
        return None

    new_data = []
    for day_cnt in range(int((constants.TODAY_DATETIME - start_date) / pd.to_timedelta("1 days"))):
        curr_dt = start_date + pd.to_timedelta(f"{day_cnt} days")
        curr_url = f'https://www.wunderground.com/history/daily/{config.WEATHER_URL_MAP[city]}/' \
                   f'date/{curr_dt.strftime("%Y-%m-%d")}'
        new_data.append(format_table(get_table(driver, curr_url, constants.HISTORY_XPATH, curr_dt),
                                    curr_dt, forecast=False))
        print(f"{'HISTORICAL DATA'.ljust(20)}::::{city.capitalize().center(15)}::::  {curr_dt.date()}")
    new_df = pd.concat(new_data, axis=0, ignore_index=True)
    new_df = fill_historical_values(new_df)
    new_df.to_sql(f"h_{city}", db_conn, if_exists='append', index=False)


def update_forecast_data(driver, city, db_conn, days):
    last_date = pd.to_datetime(
        pd.read_sql_query(f"SELECT MAX(RecordDate) FROM f_{city}", db_conn).values[0][0].split(" ")[0],
        format="%Y-%m-%d"
    )
    if last_date >= constants.TODAY_DATETIME:
        print(f'The archive for city {city.capitalize()} is already up to date. Moving on.\n', end="")
        return None
    end_date = constants.TODAY_DATETIME + pd.to_timedelta(f"{days} days")

    curr_dt = constants.TODAY_DATETIME
    curr_url = f'https://www.wunderground.com/history/daily/{config.WEATHER_URL_MAP[city]}/' \
               f'date/{curr_dt.strftime("%Y-%m-%d")}'
    new_data = []
    current_day_data = format_table(get_table(driver, curr_url, constants.FORECAST_XPATH, curr_dt),
                                    curr_dt, forecast=False)
    new_data.append(current_day_data)
    for day_cnt in range(int((end_date - constants.TODAY_DATETIME) / pd.to_timedelta("1 days"))):
        curr_dt = constants.TODAY_DATETIME + pd.to_timedelta(f"{day_cnt} days")
        curr_url = f'https://www.wunderground.com/hourly/{WEATHER_SOURCES[city]}/' \
                   f'date/{curr_dt.strftime("%Y-%m-%d")}'
        new_data.append(format_table(get_table(driver, curr_url, constants.FORECAST_XPATH, curr_dt),
                                    curr_dt, forecast=True))
        print(f"{'FORECAST DATA'.ljust(20)}::::{city.capitalize().center(10)}::::{curr_dt.date()}")
    new_df = pd.concat(new_data, axis=0, ignore_index=True)
    new_df = fill_forecast_values(new_df)
    new_df.to_sql(f"f_{city}", db_conn, if_exists='append', index=False)
