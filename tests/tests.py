from idbadapter.schedule_loader import Schedules

URL = "http://localhost:8000"

adapter = Schedules(URL)

def main():
    for pivot in adapter.from_names(works=["123"], resources=[]):
        print(pivot)    