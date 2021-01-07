#### new stock_data
import pdb
import os
import threading
if os.getcwd !="f:\\stockpro\\" : os.chdir("f:\\stockpro\\")

basicStock_path = '.\\stockData\\basicStock.txt'

dayData_path   = '.\\stockData\\dayData.pkl'
weekData_path  = '.\\stockData\\weekData.pkl'
monthData_path = '.\\stockData\\monthData.pkl'
exporting_path = '.\\stockData\\exporting.pkl'
basicData_path = '.\\stockData\\basicData.pkl'
stockListPath = '.\\stockData\\stockList.pkl'

import stock.indicatrix
import datetime
import pickle
import numpy as np
import pandas as pd

import stock.tstoken as ts 
pro = ts.pro

import stock.logger 
logger = stock.logger.StockLogging()


def checkDate(d = None):
    if d is None:
        return datetime.date.today().strftime("%Y-%m-%d")
    if isinstance(d, str):
        try:
            datetime.datetime.strptime(d, "%Y-%m-%d")
        except:
            print("请输入正确的日期")
        else:
            return d
    if isinstance(d, pd.Timestamp) or isinstance(d, datetime.date):    
        return(d.strftime("%Y-%m-%d"))

class NewData():
    def __init__(self):
        self.basicData = self.initBasicData()
        self.sList = self.initStockList()
        self.ddata = self.initData('d')
        self.wdata = self.initData('w')
        self.mdata = self.initData('m')

    def initStockList(self):
        try:
            with open(stockListPath,'rb') as fs:
                _l = pickle.load(fs)
            return _l       
        except:
            logger.info("打开股票列表错误，新建股票列表")
            _l = list(pro.stock_basic(exchange='', list_status='L', fields='ts_code').ts_code)
            with open(stockListPath,'wb') as fs:
                pickle.dump(_l, fs, pickle.HIGHEST_PROTOCOL)

    def initBasicData(self):
        with open(basicData_path,'rb') as fs:
            _data=pickle.load(fs)
        return _data

    def initData(self, ktype='d'):
        path_dict = {'d' : dayData_path, 'm' : monthData_path, 'w' : weekData_path}
        try:
            with open(path_dict.get(ktype),'rb') as fs:
                _data = pickle.load(fs)
            logger.info(f"成功读取历史数据文件，证券数量：{len(_data)}，更新日期：{ _data['000001.SZ'].index[-1]}")
            return _data
        except:
            print("数据库不存在，新建数据库")
            self.update()          
                

    def save(self, dtype='dwml'):
        path_dict = {'d' : dayData_path,'w' : weekData_path,'m' : monthData_path, 'b' : basicStock_path, 'l' : stockListPath}
        data_list = {'d' : self.ddata, 'w' : self.wdata, 'm' : self.mdata, 'b' : self.basicData, 'l' : self.sList }
        for _d in dtype:
            with open(path_dict.get(_d),'wb') as fs:
                pickle.dump(data_list.get(_d), fs, pickle.HIGHEST_PROTOCOL)
    
    def update(self, ktype = 'd'):
        self.sList = list(pro.stock_basic(exchange='', list_status='L', fields='ts_code').ts_code)
        ktype_dict = {'d':self.ddata, 'w':self.wdata, 'm':self.mdata}
        for _data in ktype_dict.values():
            if len(_data) != len(self.sList):
                ts_list = set(_data.keys()) - set(self.sList)
                ss_list = set(self.sList) - set(_data.keys())
                for k in ts_list:
                    _data.pop(k)
                for k in ss_list:
                    _data[k] = pd.DataFrame() 
        split_list = np.array_split(list(self.ddata.keys()),300)
        for k in split_list:
            t = read_thread(newUpdate, args=(k, ktype_dict.get(ktype)))
            t.start()
            t.join()
        self.wmdata()
        self.setIndicatrix('a')
        self.save()
    
    def wmdata(self):
        for k in self.ddata:
            if not self.ddata[k].empty:
                try:
                    self.wdata[k] = self.ddata[k].resample('W-FRI').agg({'open':'first','high':'max','low':'min','close':'last', 'pre_close':'first', 'change':'sum','pct_chg':'sum',
                                                                            'vol':'sum', 'amount': 'sum'})
                    # self.wdata[k]['主力净占比%'] = self.wdata[k].eval("主力净额/(volume/100 * (high + low)/2)")
                    # self.wdata[k]['超大单净占比%'] = self.wdata[k].eval("超大单净额/(volume/100 * (high + low)/2)")
                    # self.wdata[k]['大单净占比%'] = self.wdata[k].eval("大单净额/(volume/100 * (high + low)/2)")
                    self.mdata[k] = self.ddata[k].resample('M').agg({'open':'first','high':'max','low':'min','close':'last', 'pre_close':'first', 'change':'sum','pct_chg':'sum',
                                                                            'vol':'sum', 'amount': 'sum'})
                    # self.mdata[k]['主力净占比%'] = self.mdata[k].eval("主力净额/(volume/100 * (high + low)/2)")
                    # self.mdata[k]['超大单净占比%'] = self.mdata[k].eval("超大单净额/(volume/100 * (high + low)/2)")
                    # self.mdata[k]['大单净占比%'] = self.mdata[k].eval("大单净额/(volume/100 * (high + low)/2)")
                except Exception as e:
                    print(k, e)
    
    def setIndicatrix(self, k = 'd'):
        _dict = {'d':[self.ddata],'a':[self.ddata, self.wdata, self.mdata],'w':[self.wdata],'m':[self.mdata],'wm':[self.wdata, self.mdata]}
        for _set in _dict[k]:
            for _c in _set:
                if not _set[_c].empty:
                    stock.indicatrix.setIndicatrix(_set[_c]) 
    
    def exportRedThree(self, ds = None):
        dayRedThree, weekRedThree, monthRedThree = [], [], []
        dataDict = {'dayRedThree': [self.ddata, dayRedThree], 'weekRedThree': [self.wdata, weekRedThree], 'monthRedThree': [self.mdata, monthRedThree]}
        d = checkDate(ds)
        for dictk, dictv in dataDict.items():
            if dictk == 'weekRedThree':
                d = pd.Timestamp(d).to_period('W').end_time.strftime("%Y-%m-%d")
            if dictk == 'monthRedThree':
                d = pd.Timestamp(d).to_period('M').end_time.strftime("%Y-%m-%d")
            for k, _ in dictv[0].items():
                try:        
                    _ = _[:d]
                    if _.iloc[-1]['red_count'] == 3 and (_.iloc[-10:]['macd'] > 0).all():
                        dataDict[dictk][1].append(k)
                except:
                    pass
            with open(f".\\export\\{dictk}_{ds}.txt", 'w+') as fs:
                for _code in dataDict[dictk][1]:
                    fs.writelines(_code[:-3] + '\n')
        return dayRedThree, weekRedThree, monthRedThree

#============================================================================================================================
#  多线程
#============================================================================================================================
class read_thread(threading.Thread):
    def __init__(self, func, args=()):
        super(read_thread, self).__init__()
        self.func = func
        self.args = args
    
    def run(self):
        self.result = self.func(*self.args)
    
    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None     

def newUpdate(split_list, df):
    for _c in split_list:
        if _c in df and len(df[_c]) > 0:
            _date = (df[_c].index[-1] + datetime.timedelta(days=1)).strftime('%Y%m%d')   # 获取最后一天日期
        else:
            _date = '20150101'
        df[_c].sort_index(ascending=True)
        try:
            _d = updateK(_c, _date)
            assert isinstance(_d, pd.DataFrame), "下载K线数据失败"
        except Exception as e :
            logger.error(f'{e}, {_c}')
        else:
            _d['date'] = [datetime.datetime.strptime(x,"%Y%m%d") for x in _d.trade_date]
            _d.set_index('date',drop=True,inplace=True)            
            _d.sort_index(ascending=True,inplace=True) 
            _d.drop(['ts_code','trade_date'], axis = 1, inplace = True)           
            if not df[_c].index.is_unique:                           # 检查是否有重复索引
                df[_c] = df[_c][~df[_c].index.duplicated()]
            df[_c] = df[_c].append(_d, verify_integrity=True, sort=False)

def updateK(_code, _date='20180101'):  # 下载日线数据
    try:
        _df=pro.daily(ts_code=_code, start_date=_date)
        assert isinstance(_df, pd.DataFrame),  "读取数据失败"        
    except Exception as e:
        logger.error(f'{e}, {_code}')
    else:
        return _df

def deleteIndex(self, date):
    for c in self.ddata:
        self.ddata[c] = self.ddata[c][self.ddata[c].index >= date]

