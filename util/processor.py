import pandas as pd

weather_data_cols = ['City', 'RecordDate', 'WeatherDate', 'Time', 'Temperature', 'Dew Point',
                     'Humidity', 'Wind', 'Wind Speed', 'Wind Gust', 'Pressure', 'Precip.',
                     'Condition', 'IsForecast']


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
    try:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed", "Wind Gust", "Pressure", "Precip."]].astype(float)
    except ValueError:
        df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
            "Wind Gust", "Pressure", "Precip."]] = \
            df[["Temperature", "Dew Point", "Humidity", "Wind Speed",
                "Wind Gust", "Pressure", "Precip."]].replace(",", "", regex=True).astype(float)
    df.loc[df.Time.eq(24), ['Time', 'WeatherDate']] = [0, df.WeatherDate + pd.to_timedelta("1 days")]
    df.Time = df.Time.astype(int)
    df = df.reset_index()[weather_data_cols].sort_values(by=["City", "RecordDate", "WeatherDate", "Time"], ignore_index=True)
    return df
