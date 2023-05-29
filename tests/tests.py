from idbadapter.schedule_loader import Schedules
import pandas as pd
URL = "http://127.0.0.1:8000"

adapter = Schedules(URL)

def main():
    works = ['Изготовление свай', 'Монтаж свай металлических', 'Монтаж оголовков', 'Монтаж стоек, связей']
    frames = [p for p in adapter.from_names(
                                        works=works, # список работ
                                        resources=[], # список ресурсов
                                        ceil_limit=10_000, # ограничение по количеству строк на один запрос (-1 - выдать все)
                                        objects_limit=100, # ограничение на кол-во одновременно выдаваемых объектов (-1 - выдать все)
                                        crossing=False # переключение логики выбора объектов по работам и ресурсам (True - И, False - ИЛИ)
                                    )
                    ]
    df = pd.concat(frames)  
    print(df)


def select_works():
    print(adapter.get_resources_names(res_type="granulary"))
