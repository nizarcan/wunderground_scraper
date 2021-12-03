import pandas as pd


TODAY_DATETIME = pd.to_datetime(pd.to_datetime('today').date())

HISTORY_XPATH = "/html/body/app-root/app-history/one-column-layout/wu-header/sidenav/mat-sidenav-container" + \
                "/mat-sidenav-content/div/section/div[2]/div/div[5]/div/div/" + \
                "lib-city-history-observation/div/div[2]/table"

FORECAST_XPATH = "/html/body/app-root/app-hourly/one-column-layout/wu-header/sidenav/mat-sidenav-container" + \
                 "/mat-sidenav-content/div/section/div[3]/div[1]/div/div[1]/div/lib-city-hourly-forecast/div" + \
                 "/div[4]/div[1]/table"


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
