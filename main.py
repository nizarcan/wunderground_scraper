from util.archiver import export_tables, restore_backup
from util.scraper import update_historical_data, update_forecast_data
from saspy import SASsession
import threading
import config as config
from util.driver_queue import driver_queue
import sqlalchemy

db_conn = sqlalchemy.create_engine(f"sqlite:///{config.db_path}")

if config.update_data:
    # historical data updating
    threads = []
    for city in config.WEATHER_URL_MAP.keys():
        x = threading.Thread(target=update_historical_data, args=(driver_queue[city], city, db_conn, config.start_date), daemon=True)
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()
    threads = []

    for city in config.WEATHER_URL_MAP.keys():
        x = threading.Thread(target=update_forecast_data, args=(driver_queue[city], city, db_conn, 15), daemon=True)
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()

historical_data, forecast_data = restore_backup(db_conn)

if config.to_sas:
    import saspy
    sas = saspy.SASsession()
else:
    sas = ""

if config.print_results:
    combined_dfs = export_tables(historical_data, forecast_data, to_sas=config.to_sas, to_xl=config.to_xl, sas_conn=sas,
                                 xl_export_path=config.export_path, pivot_temp_cond=True)
