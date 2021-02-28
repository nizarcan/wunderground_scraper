from util.processor import fill_final_table
import pandas as pd
import os


def backup_files(historical_data, forecast_data):
    curr_dt = pd.to_datetime("today").strftime("%Y%m%d_%H%M%S")
    os.makedirs(f"_backup/{curr_dt}")
    for curr_city in historical_data.keys():
        historical_data[curr_city].to_pickle(f"_backup/{curr_dt}/historical_{curr_city.lower()}.bak")
        forecast_data[curr_city].to_pickle(f"_backup/{curr_dt}/forecast_{curr_city.lower()}.bak")


def restore_backup(backup_flag=None):
    historical_data = {}
    forecast_data = {}
    if backup_flag is None:
        last_archive = max(os.listdir("_backup"))
        for city_file in list(set([x.split("_")[1] for x in os.listdir(f"_backup/{last_archive}")])):
            historical_data[city_file.upper().split(".")[0]] = pd.read_pickle(
                f"_backup/{last_archive}/historical_{city_file}")
            forecast_data[city_file.upper().split(".")[0]] = pd.read_pickle(
                f"_backup/{last_archive}/forecast_{city_file}")
    else:
        for city_file in list(set([x.split("_")[1] for x in os.listdir(f"_backup/{backup_flag}")])):
            historical_data[city_file.upper().split(".")[0]] = pd.read_pickle(
                f"_backup/{backup_flag}/historical_{city_file}")
            forecast_data[city_file.upper().split(".")[0]] = pd.read_pickle(
                f"_backup/{backup_flag}/forecast_{city_file}")
    return historical_data, forecast_data


def export_tables(historical_df, forecast_df, to_sas=False, to_xl=True, sas_conn=None, xl_export_path="",
                  pivot_temp_cond=False):
    combined_dfs = pd.concat([forecast_df[x] for x in forecast_df.keys()], axis=0)
    combined_dfs = combined_dfs[combined_dfs.RecordDate.eq(combined_dfs.RecordDate.max())]
    combined_dfs = pd.concat([combined_dfs] + [historical_df[x] for x in historical_df.keys()], axis=0)
    combined_dfs = combined_dfs.sort_values(by=["WeatherDate", "Time", "City", "IsForecast"],
                                            ascending=[True, True, True, True], ignore_index=True)
    combined_dfs = combined_dfs.drop_duplicates(subset=["WeatherDate", "Time", "City"],
                                                keep="first").reset_index(drop=True)
    combined_dfs = fill_final_table(combined_dfs)
    if to_sas:
        sas_conn.df2sd(combined_dfs.iloc[:, :-1], 'WEATHER_DATA_WUG', 'TALTMP')
        print("Writing to SAS OK.")
    if to_xl:
        combined_dfs.iloc[:, :-1].to_excel(xl_export_path, index=False)
        print("Writing to XL OK.")
        if pivot_temp_cond:
            pivoted_df = pd.concat([combined_dfs.pivot(index=['City', 'WeatherDate'], columns='Time', values=x) for x in
                                    ['Temperature', 'Condition']]).sort_index(kind='merge').reset_index(drop=False)
            pivoted_df.insert(2, 'Data', ['Temperature', 'Condition'] * int(pivoted_df.shape[0] / 2))
            pivoted_df.to_excel(os.path.join("/".join(xl_export_path.split("/")[:-1]), "WUNDERGROUND_PIVOT.xlsx"), index=False)
    return combined_dfs
