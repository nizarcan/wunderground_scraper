from util.processor import fill_final_table
import pandas as pd
import os


def backup_files(historical_data, forecast_data):
    curr_dt = pd.to_datetime("today").strftime("%Y%m%d_%H%M%S")
    os.makedirs(f"_backup/{curr_dt}")
    for curr_city in historical_data.keys():
        historical_data[curr_city].to_pickle(f"_backup/{curr_dt}/historical_{curr_city.lower()}.bak")
        forecast_data[curr_city].to_pickle(f"_backup/{curr_dt}/forecast_{curr_city.lower()}.bak")


def restore_backup(db_conn):
    table_names = [x for x in db_conn.table_names() if x[:2] == "h_" or x[:2] == "f_"]
    historical_data = {}
    for table_name in [x for x in table_names if x[0] == "h"]:
        historical_data["_".join(table_name.split("_")[1:])] = pd.read_sql_table(table_name, db_conn)
    forecast_data = {}
    for table_name in [x for x in table_names if x[0] == "f"]:
        forecast_data["_".join(table_name.split("_")[1:])] = pd.read_sql_table(table_name, db_conn)
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
            pivoted_df = pd.concat(
                [combined_data.pivot(index=['City', 'WeatherDate'], columns='Time', values=x) for x in
                 ['Temperature', 'Condition']]).sort_index(kind='merge').reset_index(drop=False)
            pivoted_df.insert(2, 'Data', ['Temperature', 'Condition'] * int(pivoted_df.shape[0] / 2))
            pivoted_df.to_excel(os.path.join("/".join(xl_export_path.split("/")[:-1]), "WUNDERGROUND_PIVOT.xlsx"),
                                index=False)
    return combined_dfs
