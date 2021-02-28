from util.archiver import *
from util.scraper import *
from saspy import SASsession
import os

update_data = True
backup_data = True

print_results = True
to_sas = True
to_xl = True

os.chdir(os.path.dirname(os.path.abspath(__file__)))

_WEATHER_SOURCES = {
    "ANKARA": "LTAC",
    "ADANA": "tr/seyhan/LTAF",
    "ISTANBUL": "tr/pendik/LTFJ",
    "GAZIANTEP": "tr/oguzeli/LTAJ"
}

_EXPORT_PATH = "P:/BASKENT/Da_Op_Dir/Musteri_Dagitim_Operasyonlari_Grup_Mudurlugu/" \
               "6. Enerji Yönetimi ve Analiz Müdürlüğü/1. Enerji Yonetimi/00.Sıcaklık/WUNDERGROUND_DATA.xlsx"

historical_data, forecast_data = restore_backup()

if update_data:
    _START_DATE = min([historical_data[city].WeatherDate.max() for city in historical_data.keys()])
    historical_data = update_historical_data(historical_data, _WEATHER_SOURCES, _START_DATE)
    forecast_data = update_forecast_data(forecast_data, _WEATHER_SOURCES)

if backup_data:
    backup_files(historical_data, forecast_data)

sas = SASsession() if to_sas else ""

if print_results:
    combined_dfs = export_tables(historical_data, forecast_data, to_sas=to_sas, to_xl=to_xl, sas_conn=sas,
                                 xl_export_path=_EXPORT_PATH, pivot_temp_cond=True)
