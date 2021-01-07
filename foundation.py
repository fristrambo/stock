# 获取东方财富基金持股数据
import pandas as pd
import requests
import pickle
import json
import numpy as np
import os
import sys
import pdb

if os.getcwd() !='f:/stockpro':
    os.chdir('f:/stockpro')

import stock.tstoken as ts 
pro = ts.pro

import stock.logger
logger = stock.logger.StockLogging()

def preDate(_date):
    _dateDict = {'03-31':'12-31','06-30':'03-31','09-30':'06-30','12-31':'09-30'}
    if _date[5:] == '03-31':
        _pdate = str(int(_date[:4]) - 1) + '-' + _dateDict[_date[5:]]
    else:
        _pdate = _date[:5] + _dateDict[_date[5:]]
    return _pdate

def transCode(code):
    assert isinstance(code, str) and len(code) == 6, "证券代码不正确"
    if code[0] in '6':
        code = code + '.sh'
    if code[0] in '03':
        code = code + '.sz'
    return code

def transFloat(data):
    try:
        data = float(data)
    except:
        data = 0
    return data


class Foundation():
    def __init__(self):
        self.stockList = self.loadStockList()
        self.data = self.loadData()
    
    def loadStockList(self):
        stock_list_path = '.\\stockList.pkl'
        try:
            with open(stock_list_path,'rb') as fs:
                stock_list = pickle.load(fs)
        except:
            logger.info("读取股票列表失败，新建股票列表")
            stock_list = list(pro.stock_basic(exchange='', list_status='L', fields='ts_code').ts_code)
            with open(stock_list_path,'wb') as fs:
                pickle.dump(stock_list, fs, pickle.HIGHEST_PROTOCOL)
        return stock_list

    def loadData(self):
        try:
            with open(".\\emData\\Foundation.pkl", 'rb') as fs:
                _data = pickle.load(fs)
            return _data
        except:
            print("基金数据不存在！")
            return {}
    
    def save(self):
        with open(".\\emData\\Foundation.pkl", 'wb+') as fs:
            pickle.dump(self.data, fs, pickle.HIGHEST_PROTOCOL)
    
    # 更新数据， 按照 (2020,1) 传入数据
    def update(self, year, num):                
        _nDict = {1:'03-31',2:'06-30',3:'09-30',4:'12-31'}
        date = str(year) + '-' + _nDict[num]
        self.data[date] = self.upFoundation(date)
    
    def upFoundation(self, date):    
        columns = ['证券代码','证券名称','日期','产品代码','产品名称','机构代码','机构名称','产品类型代码','产品类型','持股数量','持股市值','占总股本比例','占流通股比例']
        _df = pd.DataFrame([], columns = columns)
        # codeList = loadStockList()
        for code in self.stockList:
            url = f'http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/ZLCCMX/GetZLCCMX?tkn=eastmoney&SHType=1&SHCode=&SCode={transCode(code)}&ReportDate={date}&sortField=SHCode&sortDirec=1&pageNum=1&pageSize=5000&cfg=ZLCCMX'
            _data = json.loads(requests.get(url).text)['Data']
            _d = [_.split('|') for _ in _data[0]['Data']]
            _df = _df.append(pd.DataFrame(_d, columns = columns))
        for _c  in ['持股数量','持股市值','占总股本比例','占流通股比例']:
            _df[_c] = _df[_c].apply(transFloat)
        return _df
    
def exportBasic(self, _date):
    _predate = preDate(_date)    
    _gProd = self.data[_date][0].groupby('产品名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum'})
    _gPProd = self.data[_predate][0].groupby('产品名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum'})
    _gSec =  self.data[_date][0].groupby('证券代码').agg({'证券名称':'count', '持股数量':'sum','持股市值':'sum'})
    _gPSec = self.data[_predate][0].groupby('证券名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum'})
    
    print(f'{_date}  机构投资股票数量:  {len(_gSec.index)} 只， 比上期增加 {len(_gSec.index) - len(_gPSec.index)} 只')
    print(f'{_date}  机构持股数量:  {round(_gSec["持股数量"].sum()/100000000,2)} 亿股, 比上期增加 {round(_gSec["持股数量"].sum()/100000000 - _gPSec["持股数量"].sum()/100000000, 2)} 亿股')
    print(f'{_date}  机构持股市值:  {round(_gSec["持股市值"].sum()/100000000,2)} 亿元, 比上期增加 {round(_gSec["持股市值"].sum()/100000000 - _gPSec["持股市值"].sum()/100000000, 2)} 亿元')
    print(f'{_date}  新增投资股票:  {len(set(_gSec.index) - set(_gPSec.index))} 只\n {_gSec.loc[set(_gSec.index) - set(_gPSec.index)]}')
    print(f'{_date}  退出投资股票:  {len(set(_gPSec.index) - set(_gSec.index))} 只\n {_gPSec.loc[set(_gPSec.index) - set(_gSec.index)]}')
    print(f'{_date}  持股数量增长： {self.data[_date][1].sort_values("机构数量比前期", ascending = False)[:50]}')
    print(f'{_date}  持股数量增长： {self.data[_date][1].sort_values("持股数量比前期", ascending = False)[:50]}')

def exportSec(self, _date):
    print(self.data[_date][1].sort_values('机构数量比前期', ascending = False)[:50])
    print(self.data[_date][1].sort_values('持股数量比前期', ascending = False)[:50])


# _gProd202001 = found.data['202001'].groupby('产品名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum'})
# _gSec202001 = found.data['202001'].groupby('证券名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum','占流通股比例':'sum'})

# _gProd201904 = found.data['2019-12-31'].groupby('产品名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum'})
# _gSec201904 = found.data['2019-12-31'].groupby('证券名称').agg({'证券代码':'count','持股数量':'sum','持股市值':'sum','占流通股比例':'sum'})


# len(set(_gProd202001.index) - set(_gProd201904.index))
# len(set(_gProd201904.index) - set(_gProd202001.index))

# set(_gSec202001.index) - set(_gSec201904.index(x))


# url = 'http://f10.eastmoney.com/f10_v2/BusinessAnalysis.aspx?code=sh603199'
# text = requests.get(url).text

# creat groupby stock

def creatGroupbyStock(self, _date):
    _gSec =  self.data[_date].groupby('证券代码').agg( 证券名称 = pd.NamedAgg(column = '证券名称', aggfunc = 'first'),
                                                机构数量 = pd.NamedAgg(column = '证券名称', aggfunc = 'count'),
                                                持股数量 = pd.NamedAgg(column = '持股数量', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                持股市值 = pd.NamedAgg(column = '持股市值', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                股本比例 = pd.NamedAgg(column = '占流通股比例', aggfunc = 'sum')                                                  
                                                )
    _gFound =  self.data[_date].groupby('产品名称').agg( 证券数量 = pd.NamedAgg(column = '证券名称', aggfunc = 'count'),
                                                持股数量 = pd.NamedAgg(column = '持股数量', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                持股市值 = pd.NamedAgg(column = '持股市值', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                )
    _gCom =  self.data[_date].groupby('机构名称').agg( 
                                                产品数量 = pd.NamedAgg(column = '产品名称', aggfunc = 'count'),
                                                证券数量 = pd.NamedAgg(column = '证券名称', aggfunc = lambda x : len(set(x))),
                                                持股数量 = pd.NamedAgg(column = '持股数量', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                持股市值 = pd.NamedAgg(column = '持股市值', aggfunc = lambda x :round(sum(x)/10000,2)),
                                                )    
    self.data[_date] = [self.data[_date], _gSec, _gFound, _gCom]

def addComparison(self):
    useColumns = ['机构数量','持股数量','持股市值','股本比例']
    for k in  self.data.keys():
        if preDate(k) in self.data.keys(): 
            self.data[k][1][['机构数量比前期','持股数量比前期','持股市值比前期','股本比例比前期']] = self.data[k][1][useColumns] - self.data[preDate(k)][1][useColumns]

def querySec(self, _code, _date = None):
    if _date:
        return self.data[_date][1].loc[_code]
    else:
        return pd.DataFrame(data = [self.data[k][1].loc[_code] for k in self.data.keys() if _code in self.data[k][1].index], 
                                    index = [k for k in self.data.keys() if _code in self.data[k][1].index]).sort_index(ascending = False)

