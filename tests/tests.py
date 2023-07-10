from idbadapter.schedule_loader import Schedules, GRANULARY, TYPEDLVL2, PROCESSED
import pandas as pd
import time
# URL = "http://10.32.15.31:8000"
URL = "http://127.0.0.1:8000"

    
pulls = [['изготовление свая наращивание', 'бурение лидерный скважина', 'антикоррозийный защита боковой поверхность металлический свая']]

adapter = Schedules(URL)

def main():

    # works = ['бурение лидерный скважина', 'установка в скважина свая', 'монтаж оголовник', 
    #          'монтаж ростверк и опорный конструкция под портал опора воздушный линия', 'сборка опора портал', 
    #          'установка опора портал', 'подвеска провод', 'подвеска грозозащитный трос', 'укладка активный соляный заземление', 
    #          'укладка полосовой заземление']
    # for p in adapter.from_names(
    #                                     works=works, # список работ
    #                                     resources=[], # список ресурсов
    #                                     ceil_limit=-1, # ограничение по количеству строк на один запрос (-1 - выдать все)
    #                                     objects_limit=10, # ограничение на кол-во одновременно выдаваемых объектов (-1 - выдать все)
    #                                     crossing=False, # переключение логики выбора объектов по работам и ресурсам (True - И, False - ИЛИ),
    #                                     key=PROCESSED
    #                                 ):
    #     print(p)
    #     p.fillna(0, inplace=True)
    #     print(p)
    #     raise
    # df = adapter.get_all_works_name()
    # df = adapter.get_works_by_pulls(work_pulls=[["монтаж полимерный изолятор иоспк ii ухла", "демонтаж тяга привод рабочий и значительно", "монтаж контактный нож рлнд"], 
                                                # ["демонтаж фарфоровый изолятор", "монтаж фиксировать кольцо на тяга привод рлнд"]], key=PROCESSED)
    
    
    for df in adapter.get_works_by_pulls(work_pulls=pulls, resource_list=["Автокран"], key=PROCESSED, path_to_log=r'D:\repos\idbadapter\idbadapter\empty_pulls.txt'):
        df.to_excel("test.xlsx")
        
def test_pull():
    df_pulls = {}
    for p in pulls:
        df_pulls[str(p)] = []
    for validation_dataset, pull in zip(adapter.get_works_by_pulls(work_pulls=pulls, resource_list=["Автокран"], key=PROCESSED), pulls):
        
        for _, df in validation_dataset.groupby('object_id'):
        #  if df.loc[df['is_work'] == False].shape[0] != 0:
            work_set = set(df['processed_name'].values)
            if set(pull).issubset(work_set):
                sum_work = df.loc[df['processed_name'].isin(pull)][['processed_name']+list(df.columns[10:])] # это я беру имя работ + даты, всё остальное мне не нужно
                sum_work = sum_work.groupby('processed_name').sum()
                sum_work = sum_work.reset_index()
                sum_work = sum_work.loc[:, (sum_work != 0).all(axis=0)] # тут удаляю работы, у которых все объёмы за все даты нулевые 
                if len(sum_work.columns) > 1:
                    d1 = dict(df[df.columns[10:]].sum())
                    d2 = dict(sum_work[sum_work.columns[1:]].sum())
                    shared_items = {k: d2[k] for k in d2 if d1[k] == d2[k]}
                    if len(shared_items) != 0:
                        #res = df.loc[df['is_work'] == False][['processed_name']+list(shared_items.keys())]
                        sum_work = sum_work[['processed_name']+list(shared_items.keys())]
                        #sum_work = pd.concat([sum_work, res])
                        df_pulls[str(pull)].append(sum_work)
                        