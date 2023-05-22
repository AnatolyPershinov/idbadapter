from idbadapter.schedule_loader import Schedules

URL = "http://localhost:8000"

adapter = Schedules(URL)

def main():
    works = ['Изготовление свай', 'Монтаж свай металлических', 'Монтаж оголовков', 'Монтаж стоек, связей']
    for pivot in adapter.from_names(works=works, resources=[], ceil_limit=10_000, crossing=False):
        print(pivot)    
        

if __name__ == "__main__":
    main()