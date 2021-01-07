# copy_query.py

# queryData.py 
# create query database

# stock.s.ddata[k].columns : Index(['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol',
#                                     'amount', 'ma5', 'ma10', 'ma20', 'ma30', 'diff', 'dea', 'macd', 'ad',
#                                     'wad', 'mawad', 'boll_up', 'boll_dn', 'red_three', 'stat_ma',
#                                     'stat_macd', 'stat_wad', 'stat_dea', 'stat_mawad', 'red_line',
#                                     'red_count'],
#                                     dtype='object')

# stock.f.data.keys() : dict_keys(['2019-09-30', '2019-06-30', '2017-03-31', '2017-06-30', '2017-09-30', '2017-12-31', 
#                               '2018-03-31', '2018-06-30', '2018-09-30', '2018-12-31', '2019-03-31', '2020-03-31', 
#                               '2019-12-31', '2020-09-30', '2020-06-30'], dtype='object')

# stock.f.data[k][0] ：机构投资股票总体情况
# stock.f.data[k][0].columns :  Index(['证券代码', '证券名称', '日期', '产品代码', '产品名称', '机构代码', '机构名称', '产品类型代码', '产品类型',
#                              '持股数量', '持股市值', '占总股本比例', '占流通股比例'], dtype='object')

# stock.f.data[k][1] : 每一只证券机构持股情况
# stock.f.data[k][1].columns : Index(['证券名称', '机构数量', '持股数量', '持股市值', '股本比例', '机构数量比前期', '持股数量比前期', '持股市值比前期', '股本比例比前期'], dtype='object')

# stock.f.data[k][2] : 每个机构持股情况
# stock.f.data[k][2].columns :　Index(['证券数量', '持股数量', '持股市值'], dtype='object')


 
import pdb
import os
if os.getcwd !="f:\\stockpro\\" : os.chdir("f:\\stockpro\\")

import pandas as pd
import numpy as np
import datetime
import pickle



import stock.logger 
logger = stock.logger.StockLogging()

def loadTradeDate():
    try:
        with open(".\\stockData\\tradeDate.pkl",'rb') as fs:
            _ = pickle.load(fs)            
    except:
        print("打开交易日期数据失败，新建数据库")
    else:
        return _

td = loadTradeDate()


def checkDate(d = None):
    if d is None:
        return datetime.date.today().strftime("%Y-%m-%d")
    if isinstance(d, str):
        try:
            d = datetime.datetime.strptime(d, "%Y-%m-%d")
            return d.strftime("%Y-%m-%d")
        except:
            print("请输入正确的日期")
    if isinstance(d, pd.Timestamp) or isinstance(d, datetime.date):    
        return(d.strftime("%Y-%m-%d"))

def foundationDate(d):
    d = pd.Timestamp(checkDate(d)) 
    if d < pd.Timestamp(d.year, 2, 1):
        return f"{d.year - 1}-09-30"
    if d < pd.Timestamp(d.year, 5, 1):
        return f"{d.year - 1}-12-31"
    if d < pd.Timestamp(d.year, 8, 1):
        return f"{d.year}-03-31"
    if d < pd.Timestamp(d.year, 11, 1):
        return f"{d.year}-06-30"
    return f"{d.year}-09-30"

def clearingData(data):
    if isinstance(data, str):
        data = data.replace(' ', '')
        if data  == '--' or '':
            data = np.nan
        return data
    if isinstance(data, float):
        data = round(data,2)
        return data

def clearingintData(data):
    if isinstance(data, float):
        if np.isnan(data):
            data = 0
            return data
        else:
            data = round(data)
            return data          

class queryData():
    def __init__(self, s, f, c):
        self.s = s
        self.f = f
        self.c = c
        self.queryDataPath = '.\\stockData\\queryQueryData.pkl'
        self.basicDataPath = '.\\stockData\\queryBasicData.pkl'
        self.basicData = self.initBasicData()
        self.queryData = self.initQueryData()

    def initQueryData(self):
        try:
            with open(self.queryDataPath,'rb') as fs:
                _d = pickle.load(fs)
            return  _d       
        except:
            logger.info("读取查询数据失败，新建查询数据库")
            return dict()

    def initBasicData(self):
        try:
            with open(self.basicDataPath,'rb') as fs:
                _d = pickle.load(fs)
            return  _d       
        except:
            logger.info("读取查询数据失败，新建查询数据库")
            return dict()
    
    def save(self):
        try:
            with open(self.queryDataPath,'wb') as fs:
                pickle.dump(self.queryData, fs, pickle.HIGHEST_PROTOCOL)
        except:
            logger.info("存储查询数据库失败")
        else:
            logger.info("存储查询数据库成功")
        try:
            with open(self.basicDataPath,'wb') as fs:
                pickle.dump(self.basicData, fs, pickle.HIGHEST_PROTOCOL)
        except:
            logger.info("存储查询数据库失败")
        else:
            logger.info("存储查询数据库成功")


    def update(self, d = None):
        d = checkDate(d)
        self.basicData.update({ d: self.productQueryData(d)})
        dayredthree, weekredthree, monthredthree = self.queryRedThree(d)
        self.queryData[d] = {   'dayredthree'   : self.exportFunction(dayredthree,   d), 
                                'weekredthree'  : self.exportFunction(weekredthree,  d), 
                                'monthredthree' : self.exportFunction(monthredthree, d)}


    def removeData(self, td):                   # 删除非交易日数据
        for k in list(self.basicData.keys()):
            if k not in td.values:
                del self.basicData[k]
                print(f"已从 basicData 删除 {k} 数据")
        for k in list(self.queryData.keys()):
            if k not in td.values:
                del self.queryData[k]
                print(f"已从 queryData 删除 {k} 数据")                                    

    def updateall(self):        # 更新全部数据
        startday = datetime.date.today() - datetime.timedelta(days = 30)
        endday  =  datetime.date.today() - datetime.timedelta(days = 1)
        for d in pd.date_range(startday, endday):
            d = checkDate(d)
            if d in td.values:
                self.update(d)
        self.save()

    def complete(self):        # 根据交易日历补全数据
        startday = datetime.date.today() - datetime.timedelta(days = 30)
        endday  =  datetime.date.today() - datetime.timedelta(days = 1)
        for d in pd.date_range(startday, endday):
            ds = checkDate(d)
            if ds in td.values and ds not in self.queryData:
                self.update(ds)
        self.save()

    def queryRedThree(self, d = None):
        dayredthree, weekredthree, monthredthree = self.s.exportRedThree(d)
        return dayredthree, weekredthree, monthredthree

    def exportFunction(self, stocklist: list, d = None):
        _d = self.basicData[d].loc[stocklist]
        for c in _d.columns:
            _d[c] = _d[c].map(clearingData)
        _d['机构数量'] = _d['机构数量'].map(clearingintData)
        return _d
            
    def productQueryData(self, d):
        stockColumns = ['close', 'pct_chg', 'stat_macd', 'stat_wad']
        basicColumns = ['名称','细分行业','地区','每股收益','市盈(动)','每股现金流','流通股(亿)','员工人数']
        foundationColumns = ['机构数量','持股数量','持股市值','股本比例']
        capitalColumns = ['主力净额','主力净占比']
        bdata = self.s.basicData[basicColumns]
        sddata = createDayData(self, d, stockColumns)
        swdata = createWeekData(self, d, stockColumns)
        smdata = createMonthData(self, d, stockColumns)
        cdata = self.c.dateDetail(d)[capitalColumns]
        fdata = self.f.data[foundationDate(d)][1][foundationColumns]
        data = pd.concat([bdata, sddata, swdata, smdata, fdata, cdata], axis = 1)
        logger.info(f"productQueryData, {d} is ok.")
        return data


def createDayData(self, d, col):
    inx = []
    dat = []
    for k, v in self.s.ddata.items():
        if len(v) > 30 :
            inx.append(k); dat.append(v[:d].iloc[-1])
    df = pd.DataFrame(dat, index = inx)
    df = df[col]
    df.columns =  ['现价','涨幅','日线MACD','日线WAD']
    return df

def createWeekData(self, d, col):
    inx = []
    dat = []
    for k , v in self.s.wdata.items():
        if len(v[:d]) > 4 :
            inx.append(k); dat.append(v[:pd.Timestamp(d).to_period('W').end_time].iloc[-1])
    df = pd.DataFrame(dat, index = inx)
    df = df[col[-2:]]
    df.columns = ['周线MACD','周线WAD']
    return df

def createMonthData(self, d, col):
    inx = []
    dat = []
    for k , v in self.s.mdata.items():
        if len(v[:d]) > 0:
            inx.append(k); dat.append(v[:pd.Timestamp(d).to_period('M').end_time].iloc[-1])
    df = pd.DataFrame(dat, index = inx)
    df = df[col[-2:]]
    df.columns = ['月线MACD','月线WAD']
    return df



        








#==============================================================================================================================
# import stock
# import pandas as pd
# import datetime


# bdata = self.s.basicData[basicColumns]
# def test_sdata(self):
#     startday = datetime.date.today() - datetime.timedelta(days = 30)
#     endday  =  datetime.date.today() - datetime.timedelta(days = 1)
#     stockColumns = ['close', 'pct_chg', 'stat_macd', 'stat_wad']
#     for d in pd.date_range(startday, endday): 
#         print(d)       
#         sdata = pd.DataFrame(data = [self.s.ddata[k][:d].iloc[-1][stockColumns] for k in stock.s.ddata.keys() if len(self.s.ddata[k][:d]) > 0], index = [ k for k in stock.s.ddata.keys() if len(self.s.ddata[k][:d]) > 0])

# def test_fdata(self):
#     startday = datetime.date.today() - datetime.timedelta(days = 30)
#     endday  =  datetime.date.today() - datetime.timedelta(days = 1)
#     foundationColumns = ['机构数量','持股数量','持股市值','股本比例']
#     for d in pd.date_range(startday, endday): 
#         print(d)
#         fdata = self.f.data[foundationDate(d)][1][foundationColumns]
#         print(len(fdata))

# def test_cdata(self):
#     startday = datetime.date.today() - datetime.timedelta(days = 30)
#     endday  =  datetime.date.today() - datetime.timedelta(days = 1)
#     capitalColumns = ['主力净额','主力净占比']
#     for d in pd.date_range(startday, endday): 
#         print(d)
#         cdata = self.c.dateDetail(d)[capitalColumns]


# def test_concat(self):
#     stockColumns = ['close', 'pct_chg', 'stat_macd', 'stat_wad']
#     basicColumns = ['名称','细分行业','地区','每股收益','市盈(动)','每股现金流','流通股(亿)','员工人数']
#     foundationColumns = ['机构数量','持股数量','持股市值','股本比例']
#     capitalColumns = ['主力净额','主力净占比']
#     startday = datetime.date.today() - datetime.timedelta(days = 30)
#     endday  =  datetime.date.today() - datetime.timedelta(days = 1)
#     for d in pd.date_range(startday, endday):
#         bdata = self.s.basicData[basicColumns]
#         sdata = pd.DataFrame(data = [self.s.ddata[k][:d].iloc[-1][stockColumns] for k in stock.s.ddata.keys() if len(self.s.ddata[k][:d]) > 0], index = [ k for k in stock.s.ddata.keys() if len(self.s.ddata[k][:d]) > 0])
#         cdata = self.c.dateDetail(d)[capitalColumns]
#         fdata = self.f.data[foundationDate(d)][1][foundationColumns]
#         df = pd.concat( [sdata, bdata, fdata, cdata], axis = 1)
#         print( f"{d} is ok" )

# #============================================================================================
# # 联合查询，
# # input list
# # return pd.DataFrame
# def exportFunction(stocklist:list, d = None):
#     try:
#         if d is None:
#             d = pd.Timestamp(datetime.date.today())
#         else:
#             d = pd.Timestamp(d)
#     except: 
#         print("请输入正确的日期")
#     else:
#         basicColumns = ['名称','现价','细分行业','地区','每股收益','市盈(动)','每股现金流','流通股(亿)','员工人数']
#         foundationColumns = ['机构数量','持股数量','持股市值','股本比例']
#         capitalColumns = ['主力净额','主力净占比']
#         return pd.concat([s.basicData.reindex(index = stocklist, columns = basicColumns), f.data['2020-09-30'][1].reindex(index = stocklist, columns = foundationColumns), 
#                             c.dateDetail(d).reindex(index = stocklist, columns = capitalColumns)], axis = 1)


# #============================================================================================
# # 查询输出函数
# # imput date
# # return list

# def queryRedThree(d = None):
#     dayredthree, weekredthree, monthredthree = s.exportRedThree(d)
#     return dayredthree, weekredthree, monthredthree

# #============================================================================================

# 从 stock.s.ddata 中查询某日的数据,生成一个新表
# df = pd.DataFrame(data = [stock.s.ddata[k].loc[d, stockColumns] for k in stock.s.ddata.keys() if d in stock.s.ddata[k].index], index = [ k for k in stock.s.ddata.keys() if d in stock.s.ddata[k].index])