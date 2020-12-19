from util.archiver import *
from util.scraper import *

_START_DATE = pd.to_datetime("20190101", format="%Y%m%d")
_WEATHER_SOURCES = {
    "ANKARA": "LTAC",
    "ADANA": "tr/seyhan/LTAF",
    "ISTANBUL": "tr/pendik/LTFJ",
    "GAZIANTEP": "tr/oguzeli/LTAJ"
}

_EXPORT_PATH = "P:/BASKENT/Da_Op_Dir/Musteri_Dagitim_Operasyonlari_Grup_Mudurlugu/"\
               "Tic_Kay_ Md/1. KAYIP KAÇAK/EDW İzleme/00.Sıcaklık/WUNDERGROUND_DATA.xlsx"

historical_data, forecast_data = restore_backup()

update_data = True
if update_data:
    historical_data = update_historical_data(historical_data, _WEATHER_SOURCES, _START_DATE)
    forecast_data = update_forecast_data(forecast_data, _WEATHER_SOURCES)

backup_data = True
if backup_data:
    backup_files(historical_data, forecast_data)

print_results = True
if print_results:
    create_xl_file(historical_data, forecast_data, _EXPORT_PATH)
