import pandas as pd
import requests
import json
import urllib.parse

class Schedules:
    """Get schedules from database service
    """
    def __init__(self, url):
        """Constructor
        Args:
            url (str): link to database service
        """
        
        self.url = url
        self.session = requests.Session()

    def from_schedule_ids(self, schedule_ids: list[int], ceil_limit: int=1_000):
        """method for getting schedule pivots from schedule id list

        Args:   
            schedule_ids (list[int]): list of schedules ids
            ceil_limit (int, optional): limit of records in one dataframe. Defaults to 10_000.
        """
        if len(schedule_ids) == 0:
            raise Exception("empty list of schedule ids")
        self.ceil_limit = ceil_limit
        self.objects = schedule_ids
        
        return self
        
    def from_works_or_resources(self, works_list: list[int], resource_list: list[int] = [], ceil_limit: int=1_000):
        """method for getting schedules pivots from list of works or resources ids

        Args:
            works_list (list[int]): list of works ids
            resource_list (list, optional): list of resources ids. Defaults to [].
            ceil_limit (_type_, optional): limit of records in one dataframe. Defaults to 10_000.
        """
        if len(works_list) == 0:
            raise Exception("empty works list")
        self.ceil_limit = ceil_limit
        self.works_list = works_list
        self.resource_list = resource_list
        self.objects = list({*self._get_objects_by_resource(), *self._get_objects_by_works()})

        return self
    
    def from_names(self, works: list[str], resources: list[str] = [], ceil_limit: int = 1_000, objects_limit: int = 1, crossing=False):
        """method for getting schedules by works names list

        Args:
            work_name_list (list[str]): lists of basic works names 
            ceil_limit (int, optional): limit of records in one dataframe. Defaults to 1_000.
        """
        if len(works) == 0 and len(resources) == 0:
            raise Exception("Empty works list")
        self.ceil_limit = ceil_limit
        self.objects_limit = objects_limit
        self.works_list = self._get_works_ids_by_names(works)
        self.resource_list = self._get_resource_ids_by_names(resources)
        
        if crossing:
            self.objects =list(set(self._get_objects_by_resource()).intersection(set(self._get_objects_by_works())))
        else:        
            self.objects = list({*self._get_objects_by_resource(), *self._get_objects_by_works()})
            
        if len(self.objects) == 0:
            raise Exception("Objects not found")
        
        return self
           
    def _get_works_ids_by_names(self, work_name_list):
        data = json.dumps(work_name_list)
        response = self.session.post(urllib.parse.urljoin(self.url, "work/get_basic_works_ids"), data=data)
        if "detail" in response.json(): 
            return []
        return response.json()
    
    def _get_resource_ids_by_names(self, resource_names_list):
        if len(resource_names_list) == 0:
            return []
        data = json.dumps(resource_names_list)
        response = self.session.post(urllib.parse.urljoin(self.url, "resource/get_basic_resource_ids"), data=data)
        if "detail" in response.json():
            return []
        return response.json()
    
    def _get_objects_by_resource(self):
        if len(self.resource_list) == 0:
            return []
        data = json.dumps(self.resource_list)
        response = self.session.post(urllib.parse.urljoin(self.url, "resource/schedule_ids"), data=data)
        return response.json()
    
    def _get_objects_by_works(self):
        if len(self.works_list) == 0:
            return []
        data = json.dumps(self.works_list)
        response = self.session.post(urllib.parse.urljoin(self.url, "work/schedule_ids"), data=data)
        return response.json()
   
    def __iter__(self):
        return SchedulesIterator(self.objects, self.session, self.url, self.ceil_limit, self.objects_limit)


class SchedulesIterator:
    def __init__(self, objects, session, url, ceil_limit, objects_limit):
        self.objects = objects
        self.session = session
        self.objects_limit = objects_limit if objects_limit != -1 else len(objects)
        self.url = url
        self.ceil_limit = ceil_limit
        self.index = 0
        self.start_date = "1970-1-1"
        self.object_slice = self.objects[self.index:self.index+self.objects_limit]

    def _select_works_from_db(self):
        data = json.dumps({
            "object_id": self.object_slice,
            "start_date": self.start_date,
            "max_work_statuses": self.ceil_limit
        })

        response = self.session.post(urllib.parse.urljoin(self.url, "schedule/works_by_schedule"), data=data)
        works = response.json()
        df = pd.DataFrame(works)       

        return df

    def _select_resources_from_db(self, start_date, finish_date):
        data = json.dumps({
            "object_id": self.object_slice,
            "start_date": start_date,
            "finish_date": finish_date
        })
        response = self.session.post(urllib.parse.urljoin(self.url, "schedule/resources_by_schedule"), data=data)
        resources = response.json()

        df = pd.DataFrame(resources)
        
        return df
    
    def __next__(self):
        if len(self.object_slice) == 0:
            raise StopIteration
        
        try:
            works_df = self._select_works_from_db()
            if len(works_df) == self.ceil_limit:
                self.start_date = works_df.date.max()
                works_df = works_df[works_df.date != self.start_date]
                res_df = self._select_resources_from_db(works_df["date"].min(), works_df["date"].max())    
            else:
                res_df = self._select_resources_from_db(works_df["date"].min(), works_df["date"].max())  
                self.start_date = "1970-1-1"
                self.index += self.objects_limit
                self.object_slice = self.objects[self.index:self.index+self.objects_limit]
                    
        except IndexError:
            raise StopIteration
        
        df = pd.concat([works_df, res_df])

        df = self.convert_df(df)

        return df

    @staticmethod
    def convert_df(df: pd.DataFrame):
        result = df.pivot_table("fraction", ["is_work", "basic_name", "name", "object_id", "object_name", "full_fraction", ], "date")
        return result.reset_index()
