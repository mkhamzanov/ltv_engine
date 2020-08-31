import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import calendar
import sys
import json

class File_Transform():
    def __init__(self, path, input_dict):
        self.path = path
        self.error = ''
        self.general_dict = {
            'Дата платежа' : 'PAY_DATE',
            'Объем платежа' : 'PAY_AMOUNT',
            'ID пользователя' : 'USER_ID',
            'Дата регистрации' : 'REG_DATE',
            'Страна' : 'COUNTRY',
            'Источник' : 'SOURCE',
            'Платформа' : "PLATFORM"
        }
        
        
        self.general_dict_inv = {x : y for y,x in self.general_dict.items()}

        
        self.general_dtypes = {
             'PAY_DATE' : 'date',
            'PAY_AMOUNT' : 'float',
            'USER_ID' : 'str',
             'REG_DATE' : 'date',
            'COUNTRY' : 'str',
            'SOURCE' : 'str',
             "PLATFORM" : 'str'
        }
        self.input_dict = input_dict
    
    def check_separation(self):
        for sep in ['\t', ',', ';']:
            df = pd.read_csv(self.path, sep=sep, encoding='cp1251')
            if df.shape[1]>1:
                self.df =  df
                
                
    def change_column_names(self):
        map_dict = {y : self.general_dict[x] for x,y in self.input_dict.items()}
        for key in self.general_dtypes:
            if not key in map_dict.values():
                self.general_dtypes[key] = False
        self.df = self.df.rename(columns=map_dict)[list(map_dict.values())]
        
    
    def change_str_columns(self):
        for key in self.general_dtypes:
            if self.general_dtypes[key]=='str':
                self.df[key] = self.df[key].astype(str)

                
                
    def change_float_columns(self):
        for key in self.general_dtypes:
            if self.general_dtypes[key]=='float':
                try:
                    self.df[key] = self.df[key].astype(str).apply(lambda x: x.replace(',', '.')).astype(float)
                except:
                    self.path+=f"Problem with column : {self.general_dict_inv[key]}"
                    
                    
    def change_datetime_columns(self):
        timestamp = False
        for key in self.general_dtypes:
            if self.general_dtypes[key]=='date':
                try:
                    self.df[key + '_TS'] = self.df[key].astype(int)
                    timestamp = True
                except:
                    pass
                if not timestamp:
                    self.df[key] = self.df[key].astype(str).apply(lambda x: x.replace('.', '-')[0:10])
                    self.df[key + '_YEAR'] = self.df[key].astype(str).apply(lambda x: x[-4:])
                    try:
                        self.df[key + '_YEAR'] = self.df[key + '_YEAR'].astype(int)
                        self.df[key] = pd.to_datetime(self.df[key], format='%d-%m-%Y')
                        self.df = self.df.drop(key + '_YEAR', axis=1)
                    except:
                        self.df[key] = pd.to_datetime(self.df[key], format='%Y-%m-%d')
                        self.df = self.df.drop(key + '_YEAR', axis=1)
                    
                    self.df[key + '_TS'] = pd.to_timedelta(self.df[key], unit='ns').dt.total_seconds().astype(int)
                    
    def save_df(self):
        new_path = self.path.split('csv')[0]
        new_path = new_path[:-1] + '_transformed.csv'
        self.df.to_csv(new_path, index=False)
        
        
if __name__ == "__main__":
    
    # path = sys.argv[0]
    # json_input_dict = sys.argv[1]
    # input_dict = (json.loads(json_input_dict))
    
    path = 'test.csv'
    input_dict = {
        'Дата платежа' : 'Date',
        'Объем платежа' : 'SumRevenue_clean',
        'ID пользователя' : 'BinDeviceID',
        'Дата регистрации' : 'Cohort',
        'Страна' : 'Country',
        'Источник' : '_ms_common',
#         'Платформа' : "PlatformType"
    }    
    
    Transf = File_Transform(path, input_dict)
    Transf.check_separation()
    Transf.change_column_names()
    Transf.change_str_columns()
    Transf.change_float_columns()
    Transf.change_datetime_columns()
    Transf.save_df()
    error = Transf.error
    sys.stdout.write(error)
    sys.exit(0)