from util.processor import fill_final_table
import config as config
import pandas as pd
import os


def restore_backup(db_conn):
    table_names = list(config.WEATHER_URL_MAP.keys())
    historical_data = {}
    for table_name in table_names:
        historical_data[table_name] = pd.read_sql_table(f"h_{table_name}", db_conn)
        historical_data[table_name]['WeatherDate'] = pd.to_datetime(historical_data[table_name]['WeatherDate'])
        historical_data[table_name]['RecordDate'] = pd.to_datetime(historical_data[table_name]['RecordDate'])
    forecast_data = {}
    for table_name in table_names:
        forecast_data[table_name] = pd.read_sql_table(f"f_{table_name}", db_conn)
        forecast_data[table_name]['WeatherDate'] = pd.to_datetime(forecast_data[table_name]['WeatherDate'])
        forecast_data[table_name]['RecordDate'] = pd.to_datetime(forecast_data[table_name]['RecordDate'])

    return historical_data, forecast_data


def export_tables(historical_data, forecast_data, to_sas=False, to_xl=True, sas_conn=None, xl_export_path="",
                  pivot_temp_cond=False):
    combined_data = []
    for city in historical_data.keys():
        temp = historical_data[city].copy()
        temp.insert(0, 'City', city.capitalize())
        combined_data.append(temp)

    for city in forecast_data.keys():
        temp = forecast_data[city].copy()
        temp = temp[temp.RecordDate.eq(temp.RecordDate.max())]
        temp.insert(0, 'City', city.capitalize())
        combined_data.append(temp)
    combined_data = pd.concat(combined_data, axis=0, ignore_index=True)
    combined_dfs = combined_data.sort_values(by=["WeatherDate", "Time", "City"],
                                             ascending=[True, True, True], ignore_index=True)
    combined_data = combined_data.drop_duplicates(subset=["WeatherDate", "Time", "City"],
                                                  keep="first").reset_index(drop=True)
    combined_data = fill_final_table(combined_data)
    if to_sas:
        sas_conn.df2sd(combined_data, 'WEATHER_DATA_WUG', 'TALTMP')
        print("Writing to SAS OK.")
    if to_xl:
        combined_data.to_excel(xl_export_path, index=False)
        print("Writing to XL OK.")
        if pivot_temp_cond:
            pivot_cols = list(combined_data.columns[3:])
            pivoted_df = pd.concat(
                [combined_data.pivot(index=['City', 'WeatherDate'], columns='Time', values=x) for x in
                 pivot_cols]).sort_index(kind='merge').reset_index(drop=False)
            pivoted_df.insert(2, 'Data', pivot_cols * int(pivoted_df.shape[0] / len(pivot_cols)))
            pivoted_df.to_excel(os.path.join("/".join(xl_export_path.split("/")[:-1]), "WUNDERGROUND_PIVOT.xlsx"),
                                index=False)
            print("Writing pivot OK.")
    return combined_dfs
