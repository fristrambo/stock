#### 下载东方财富 资金流向数据
import pdb
import os
import threading
if os.getcwd !="f:\\stockpro\\" : os.chdir("f:\\stockpro\\")


import datetime
import pickle
import numpy as np
import pandas as pd
import json
import threading
import requests

import stock.logger
logger = stock.logger.StockLogging()

capitalPath = '.\\stockData\\capital.pkl'
stockListPath = '.\\stockList.pkl'


import stock.tstoken as ts 
pro = ts.pro

class Capital():    
    def __init__(self):
        self.stockList = self.loadStockList()   # 获取股票列表
        self.capital = self.init()

    def loadStockList(self):
        try:
            with open(stockListPath,'rb') as fs:
                stock_list = pickle.load(fs)
        except:
            logger.info("读取股票列表失败，新建股票列表")
            stock_list = list(pro.stock_basic(exchange='', list_status='L', fields='ts_code').ts_code)
            with open(stockListPath,'wb') as fs:
                pickle.dump(stock_list, fs, pickle.HIGHEST_PROTOCOL)
        return stock_list
        
    def init(self):
        try:
            with open(capitalPath,'rb') as fs:
                _ = pickle.load(fs)            
        except:
            print("打开资金数据库失败，新建数据库")
            _ = { k : pd.DataFrame() for k in self.stockList}
        self.capital = _
        if (newStock := set(self.stockList) - set(self.capital.keys())):
            self.capital.update({_ : pd.DataFrame() for _ in newStock}) 
        return _
    
    def save(self):
        with open(capitalPath,'wb') as fs:
            pickle.dump(self.capital, fs, pickle.HIGHEST_PROTOCOL)
    
    def update(self):
        splitList = np.array_split(self.stockList, 300)
        for k in splitList:
            t = read_thread(update, args = (self.capital, k))
            t.start()
            t.join()
    
    def dateDetail(self, d):
        # 按日期查询资金流入情况
        return pd.DataFrame(data = [ self.capital[k][:d].iloc[-1] for k in self.capital if len(self.capital[k][:d]) > 0 ], index = [ k for k in self.capital if len(self.capital[k][:d]) > 0])  
    
    def queryDate(self, _date):
        # pdb.set_trace()
        tDay = self.dateDetail(_date)   
        clist = list(tDay.sort_values('主力净额', ascending = False)[:20].index) + list(tDay.sort_values('主力净占比', ascending = False)[:20].index)
        return clist

    def querySustain(self, _date):
        # 查询连续三天资金流入的股票
        return [k for k in self.capital if len(self.capital[k].loc[:_date][-3:]) == 3 and (self.capital[k].loc[:_date][-3:]['主力净额'] > 0).all()]



#============================================================================================================================
#  多线程
#============================================================================================================================
class read_thread(threading.Thread):
    def __init__(self, func, args=()):
        super(read_thread,self).__init__()
        self.func = func
        self.args = args
    
    def run(self):
        self.result = self.func(*self.args)
    
    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None     

def update(df, k):
    for _c in k:        
        try:
            _d = updateCapital(_c)
            assert isinstance(_d, pd.DataFrame), f"下载 {_c} 资金数据失败"
        except Exception as e:
            logger.error(f'{e}, {_c}')
        else:
            # if not df[_c].index.is_unique:                           # 检查是否有重复索引                
            df[_c] = df[_c].append(_d, verify_integrity = False, sort = False)
            df[_c] = df[_c][~df[_c].index.duplicated()]          # 删除重复索引
            df[_c].sort_index(ascending = True, inplace = True)
            print(f'下载 {_c} 资金流量数据 successful') 

def updateCapital(_c):  # 下载资金数据
    header = '日期, 主力净额, 小单净额, 中单净额, 大单净额, 超大单净额, 主力净占比, 小单净占比, 中单净占比, 大单净占比, 超大单净占比, 收盘价, 涨跌幅, 周流入, 月流入'
    headerColumns = [k.strip() for k in header.split(',')]
    newHqDomain = '//push2his.eastmoney.com/'
    procotol = 'http:'
    data = {'lmt': "0",
            'klt': "101",
            'secid': hqcode(_c),
            'fields1': "f1,f2,f3,f7",
            'fields2': "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
            'ut': "b2884a393a59ad64002292a3e90d46a5"
            }
    url = procotol + newHqDomain + 'api/qt/stock/fflow/daykline/get?' + '&'.join([ f'{k}={data[k]}' for k in data])
    try:
        response = requests.get(url).text
    except:
        print(_c, ":  链接失败")
        _d = pd.DataFrame()
        return _d
    else:
        try:
            _arr = json.loads(response)['data']['klines']
            _d = pd.DataFrame([k.split(',') for k in _arr], columns = headerColumns)
        except AttributeError:
            print(_c, ":  没有交易数据")
            # emptyList.append(_c)
        else:
            _d['日期']=_d['日期'].apply(cl_strptime)
            _d.set_index(_d['日期'], drop=True, inplace=True)
            del _d['日期']
            _d.sort_index(ascending=True,inplace=True)
            _d = _d[~_d.index.duplicated(keep='first')]
            _d = _d.astype(float)
            return _d

#================================================================================
# 辅助函数
#================================================================================

def cl_strptime(_d):
    return datetime.datetime.strptime(_d,'%Y-%m-%d')

def cl_strftime(_d):
    return datetime.datetime.strftime(_d,'%Y-%m-%d')


def hqcode(symbol):
    Market_dict = {'6':'1','3':'0','0':'0'}
    return f'{Market_dict[symbol[0]]}.{symbol[:6]}'



    


