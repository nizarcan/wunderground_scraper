import pandas as pd

weather_data_cols = ['City', 'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point',
                     'Humidity', 'Wind', 'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.',
                     'Condition']


def fill_historical_values(df):
    df = df.copy()
    try:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]].astype(float)
    except ValueError:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
            "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
                "Wind Gust", "Pressure", "Precip."]].replace(",", "", regex=True).astype(float)
    df.Time = df.Time.astype(int)
    df = df.sort_values(by=["WeatherDate", "Time"], ascending=True, ignore_index=True)
    df["WeatherDate"] += pd.to_timedelta(df.Time.astype(str) + " hours")
    df.set_index("WeatherDate", inplace=True)
    df = df.resample("h").first()
    while df.isna().sum().sum() > 0:
        df = df.fillna(df.shift(24))
    df = df.reset_index()[weather_data_cols].sort_values(by=["WeatherDate", "Time"], ignore_index=True)
    df.Time = df.WeatherDate.dt.hour
    df.WeatherDate = pd.to_datetime(df.WeatherDate.dt.date)
    return df


def fill_forecast_values(df):
    df = df.copy()
    df['Precip.'] = df['Precip.'].str.split(" ", expand=True)[0]
    try:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]].astype(float)
    except ValueError:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
            "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
                "Wind Gust", "Pressure", "Precip."]].replace(",", "", regex=True).astype(float)
    df.Time = df.Time.astype(int)
    df = df.reset_index()[weather_data_cols].sort_values(by=["RecordDate", "WeatherDate", "Time"],
                                                         ignore_index=True)
    return df


def fill_final_table(df):
    sep_tables = {}
    for city in df.City.unique():
        sep_tables[city] = df[df.City.eq(city)].copy()
        sep_tables[city] = sep_tables[city].sort_values(by=["WeatherDate", "Time"], ascending=True, ignore_index=True)
        sep_tables[city]["WeatherDate"] += pd.to_timedelta(sep_tables[city].Time.astype(str) + " hours")
        sep_tables[city].set_index("WeatherDate", inplace=True)
        sep_tables[city] = sep_tables[city].resample("h").first()
        while sep_tables[city].isna().sum().sum() > 0:
            sep_tables[city] = sep_tables[city].fillna(sep_tables[city].shift(24))
        sep_tables[city] = sep_tables[city].reset_index()[weather_data_cols].sort_values(by=["WeatherDate", "Time"],
                                                                                         ignore_index=True)
        sep_tables[city].Time = sep_tables[city].WeatherDate.dt.hour
        sep_tables[city].WeatherDate = pd.to_datetime(sep_tables[city].WeatherDate.dt.date)
    final_table = pd.concat([sep_tables[city] for city in sep_tables.keys()], axis=0, ignore_index=True)
    final_table = final_table.sort_values(by=["WeatherDate", "Time", "City"],
                                          ascending=[True, True, True], ignore_index=True)
    return final_table
