# trade_date.py
import pdb
import os
if os.getcwd !="f:\\stockpro\\" : os.chdir("f:\\stockpro\\")

import stock.tstoken as ts 
pro = ts.pro

import pickle
import pandas as pd


import os
if os.getcwd !="f:\\stockpro\\" : os.chdir("f:\\stockpro\\")

def formatStrDate(datestr):
    return ( f"{datestr[:4]}-{datestr[4:-2]}-{datestr[-2:]}" )


class tradeDate():
    def __init__(self):
        self.path = ".\\stockData\\tradeDate.pkl"
        self.data = self.load()
    
    def load(self):
        try:
            with open(self.path,'rb') as fs:
                _ = pickle.load(fs)            
        except:
            print("打开交易日期数据失败，新建数据库")
            _ = self.update()
        return _
    
    def update(self):
        _data =  pro.trade_cal(exchange = "SSE", start_date = "2020-01-01")
        _date = _data[_data['is_open'] == 1]['cal_date'].map(formatStrDate)
        return _date
    
    def save(self):
        with open(self.path,'wb') as fs:
            pickle.dump(self.data, fs, pickle.HIGHEST_PROTOCOL)




