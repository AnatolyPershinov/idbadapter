from idbadapter.schedule_loader import Schedules

URL = "http://127.0.0.1:8000"

adapter = Schedules(URL)

def main():
    for pivot in adapter.from_names(works=["Монтаж привода ПРН3-10 УХЛ1", "Монтаж шлейфов и спиральных зажимов", "тест"], resources=["тест"]):
        print(pivot)    