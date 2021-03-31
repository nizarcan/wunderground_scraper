from util.archiver import export_tables, restore_backup
from util.scraper import update_historical_data, update_forecast_data
from saspy import SASsession
import sqlalchemy
import threading
import os


main_dir = os.path.dirname(os.path.abspath(__file__))
db_path = f"{os.path.dirname(os.path.abspath(__file__))}/data/wunderground_data.db"
export_path = f"{os.path.dirname(os.path.abspath(__file__))}/data/WUNDERGROUND_DATA.xlsx"
os.chdir(main_dir)
db_conn = sqlalchemy.create_engine(f"sqlite:///{db_path}")

cities = ['adana', 'ankara', 'gaziantep', 'istanbul']

update_data = True
backup_data = True

print_results = True
to_sas = True
to_xl = True


if update_data:
    # historical data updating
    print("<----------- STARTING HISTORICAL DATA UPDATE ----------->")
    threads = []
    for city in cities:
        x = threading.Thread(target=update_historical_data, args=(city, db_conn), daemon=True)
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()
    print("<----------- FINISHED HISTORICAL DATA UPDATE ----------->\n\n", end="")

    print("<------------ STARTING FORECAST DATA UPDATE ------------>")
    threads = []
    for city in cities:
        x = threading.Thread(target=update_forecast_data, args=(city, db_conn, 15), daemon=True)
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()
    print("<------------ FINISHED FORECAST DATA UPDATE ------------>\n\n", end="")

historical_data, forecast_data = restore_backup(db_conn)

sas = SASsession() if to_sas else ""

if print_results:
    combined_dfs = export_tables(historical_data, forecast_data, to_sas=to_sas, to_xl=to_xl, sas_conn=sas,
                                 xl_export_path=export_path, pivot_temp_cond=True)
