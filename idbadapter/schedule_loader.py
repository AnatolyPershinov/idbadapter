import pandas as pd
import requests
import json
from urllib.parse import urljoin

GRANULARY = {"column": "granulary_name", "table": "granulary_works", "id": "id_granulary_work"}

PROCESSED = {"column": "processed_name", "table": "processed_works", "id": "id_processed_work"}

TYPEDLVL2 = {"column": "typed_lvl2_name", "table": "typed_lvl2_works", "id": "id_typed_lvl2_work"}

ALL = "all"


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
    
    def from_names(self, works: list[str], resources: list[str] = [], ceil_limit: int = 1_000, objects_limit: int = 1, crossing=False, key=GRANULARY):
        """method for getting schedules by works names list

        Args:
            work_name_list (list[str]): lists of basic works names 
            ceil_limit (int, optional): limit of records in one dataframe. Defaults to 1_000.
        """
        if len(works) == 0 and len(resources) == 0:
            raise Exception("Empty works list")
        self.ceil_limit = ceil_limit
        self.objects_limit = objects_limit
        self.works_list = works
        self.resource_list = self._get_resource_ids_by_names(resources)
        
        if crossing:
            self.objects = list(set(self._get_objects_by_resource()).intersection(set(self._get_objects_by_names(key))))
        else:        
            self.objects = list({*self._get_objects_by_resource(), *self._get_objects_by_names(key)})
            
        if len(self.objects) == 0:
            raise Exception("Objects not found")
        
        return self
           
    def get_resources_names(self, res_type="granulary"):
        queries = {
            "granulary": "SELECT DISTINCT name FROM basic_resources",
        }
        if res_type not in queries:
            raise ValueError(f"Incorrect work_type argument. {res_type}")
        
        data = json.dumps({"body": queries[res_type]})        
        response = self.session.post(urljoin(self.url, "query/select"), data=data)
        
        return [k[0] for k in response.json()]
        
    def get_works_names(self, work_type=GRANULARY):
        query = f"SELECT DISTINCT name FROM {work_type['table']}"
        df = self._execute_query(query)
        return df
    
    def _get_works_ids_by_names(self, work_name_list):
        data = json.dumps(work_name_list)
        response = self.session.post(urljoin(self.url, "work/get_basic_works_ids"), data=data)
        if "detail" in response.json(): 
            return []
        return response.json()
    
    def _get_resource_ids_by_names(self, resource_names_list):
        if len(resource_names_list) == 0:
            return []
        data = json.dumps(resource_names_list)
        response = self.session.post(urljoin(self.url, "resource/get_basic_resource_ids"), data=data)
        if "detail" in response.json():
            return []
        return response.json()
    
    def _get_objects_by_resource(self):
        if len(self.resource_list) == 0:
            return []
        data = json.dumps(self.resource_list)
        response = self.session.post(urljoin(self.url, "resource/schedule_ids"), data=data)
        return response.json()
    
    def _get_objects_by_works(self):
        if len(self.works_list) == 0:
            return []
        data = json.dumps(self.works_list)
        response = self.session.post(urljoin(self.url, "work/schedule_ids"), data=data)
        return response.json()
    
    def _get_objects_by_names(self, key):
        key_id = key["id"]
        key_table = key["table"]
        
        query = f"""
            select DISTINCT bo.id as id from {key_table} pw 
            join work_info wi ON wi.{key_id} = pw.id
            join work_completeness wc on wc.id_work = wi.id_work 
            join object_day od on od.id = wc.id_object_day 
            join object_schedules os on os.id = od.id_object_schedules 
            join building_objects bo on bo.id = os.id_building_object 
            where pw.name in ({",".join(map(lambda x: f"'{x}'", self.works_list))})
            """
        
        df = self._execute_query(query)
        return df["id"].values.tolist()

    
    def _execute_query(self, stmt) -> pd.DataFrame:
        data = json.dumps({
            "body": stmt.replace('\n', "").replace("\t", "")
        })
        response = self.session.post(urljoin(self.url, "query/select"), data=data)
        
        result = response.json()
        df = pd.DataFrame(result)
        return df
    
    def get_all_works_name(self):
        query = f"""
        select DISTINCT w.name as work_name, gw.name as granulary_name, tlw.name as lvl2_name, pn.name as processed_name from works w
        join work_info wi on w.id = wi.id_work 
        join granulary_works gw on gw.id = wi.id_granulary_work 
        join typed_lvl2_works tlw on tlw.id = wi.id_typed_lvl2_work 
        join processed_works pn on pn.id = wi.id_processed_work"""
        
        df = self._execute_query(query)
        return df
    
    def get_works_by_pulls(self, work_pulls: list, resource_list: list = [], key=GRANULARY, path_to_log="empty_pull.txt"):
        self.path_to_log = path_to_log
        for pull in work_pulls:
            query = f"""with date_cte (date, object_id)
                as (
                    (select date, object_id from works_names_mv wnm
                    where wnm.{key["column"]} in ({','.join(map(lambda x: f"'{x}'", pull))})
                    group by object_id, date 
                    having count(distinct wnm.{key["column"]}) = {len(pull)}
                    )
                    except (
                    select date, object_id from works_names_mv wnm
                    where wnm.{key["column"]} not in ({','.join(map(lambda x: f"'{x}'", pull))})
                    group by object_id, date)
                )
                select true as is_work, * from works_names_mv wsv
                join date_cte on date_cte.object_id = wsv.object_id
                and wsv.date = date_cte.date
                where wsv.object_id = date_cte.object_id
                union
                select false as is_work, * from resource_names_mv rnm
                join date_cte on date_cte.object_id = rnm.object_id
                and rnm.date >= date_cte.date
                where rnm.object_id = date_cte.object_id"""

                
            if len(resource_list) != 0:
                query += f""" and rnm.name in ({",".join(map(lambda x: f"'{x}'", resource_list))})"""   
            try:
                df = self._execute_query(query)
            except ValueError:
                print("jsondecodeerror occurred", pull)
                continue 

            if df.empty:
                with open(self.path_to_log, "a", encoding="UTF-8") as f:
                    print("pull not found", pull, file=f)
                    print("empty df. pull not found")
                continue
                
            df["full_fraction"] = df["physical_volume"]
                      
                
            yield SchedulesIterator.convert_df(df)

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

    def _execute_query(self, stmt) -> pd.DataFrame:
        data = json.dumps({
            "body": stmt.replace('\n', "").replace("\t", "")
        })
        response = self.session.post(urljoin(self.url, "query/select"), data=data)
        result = response.json()
        df = pd.DataFrame(result)
        return df
    
    def _select_works_from_db(self):
       
        query = f"""
        select true as is_work, * from works_names_mv wsv
        where wsv.object_id in ({",".join(map(str, self.object_slice))})
        and wsv.date >= '{self.start_date}'
        """
        if self.ceil_limit != -1:
            query += f"limit {self.ceil_limit}"
            
        df = self._execute_query(query)
        return df

    def _select_resources_from_db(self, start_date, finish_date):
        data = json.dumps({
            "object_id": self.object_slice,
            "start_date": start_date,
            "finish_date": finish_date
        })
        response = self.session.post(urljoin(self.url, "schedule/resources_by_schedule"), data=data)
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
            elif self.ceil_limit == -1 or len(works_df) != self.ceil_limit:
                res_df = self._select_resources_from_db(works_df["date"].min(), works_df["date"].max())  
                self.start_date = "1970-1-1"
                self.index += self.objects_limit
                self.object_slice = self.objects[self.index:self.index+self.objects_limit]
            df = pd.concat([works_df, res_df])
            df = self.convert_df(df)               
        except IndexError:
            raise StopIteration

        return df

    @staticmethod
    def convert_df(df: pd.DataFrame):
        other_columns = [c for c in df.columns if c not in ["fraction", "date"]]
        result = df.pivot_table("fraction", ['is_work', 'processed_name', 'granulary_name', 'typed_lvl2_name', 'name', 'physical_volume', 'object_name', 'measurement_unit'], "date")
        return result.reset_index()
