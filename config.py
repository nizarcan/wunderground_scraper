import os

main_dir = os.path.dirname(os.path.abspath(__file__))
db_path = f"{os.path.dirname(os.path.abspath(__file__))}/data/wunderground_data.db"
export_path = f"{os.path.dirname(os.path.abspath(__file__))}/data/WUNDERGROUND_DATA.xlsx"
os.chdir(main_dir)

cities = ['adana', 'ankara', 'gaziantep', 'istanbul']

start_date = "2018-01-01"

WEATHER_URL_MAP = {
    "ankara": "LTAC",
    "adana": "tr/seyhan/LTAF",
    "istanbul": "tr/pendik/LTFJ",
    "gaziantep": "tr/oguzeli/LTAJ"
}

update_data = True
backup_data = True

print_results = True
to_sas = False
to_xl = True
