from idbadapter.schedule_loader import Schedules
import pandas as pd
URL = "http://localhost:8000"

adapter = Schedules(URL)

def main():
    works = ['Изготовление свай', 'Монтаж свай металлических', 'Монтаж оголовков', 'Монтаж стоек, связей']
    resources = ['Самосвал']
    frames = [p for p in adapter.from_names(
                                        works=works, # список работ
                                        resources=resources, # список ресурсов
                                        ceil_limit=50_000, # ограничение по количеству строк на один запрос (-1 - выдать все)
                                        objects_limit=50, # ограничение на кол-во одновременно выдаваемых объектов (-1 - выдать все)
                                        crossing=False # переключение логики выбора объектов по работам и ресурсам (True - И, False - ИЛИ)
                                    )
                    ]
    df = pd.concat(frames)  
    print(df)

if __name__ == "__main__":
    main()