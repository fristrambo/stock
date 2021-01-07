# basicData.py
import os
os.chdir("f:\\stockpro\\")

import re
import pandas as pd
import stock.tstoken as ts
pro = ts.pro

import pickle 
import stock.logger as logger
logger = logger.StockLogging()


class tushareBasicData():
    def __init__(self):
        self.stockListPath = r'f:\\stockpro\\stockData\\stockList.pkl'
        self.dataPath = r'f:\\stockpro\\stockData\\tushareBasicData.pkl'
        self.data = self.readData()
    
    def readData(self):
        try:
            with open(self.dataPath,'rb') as fs:
                d = pickle.load(fs)
            return d       
        except:
            logger.info("打开 tushareBasicData 错误，新建数据")
            return self.updateBasicData()          
    
    def updateBasicData(self):
        df_szse = pro.stock_company(exchange='SZSE', fields='ts_code, reg_capital, province, city, setup_date, employees, chairman, manager, secretary, introduction, main_business, business_scope')
        df_sse = pro.stock_company(exchange='SSE', fields='ts_code, reg_capital, province, city, setup_date, employees, chairman, manager, secretary, introduction, main_business, business_scope')
        data = pd.concat([df_sse, df_szse])
        data.set_index('ts_code', drop = True, inplace = True)
        data.fillna('', inplace = True)
        self.save()
        return data
    
    def save(self):
        try:
            with open(self.dataPath,'wb') as fs:
                pickle.dump(self.data, fs, pickle.HIGHEST_PROTOCOL)
        except:
            logger.info("存储 tushareBasicData 失败")
        else:
            logger.info("存储 tushareBasicData 成功")
    
    def queryData(self, *str):
        self.data.fillna('', inplace = True)
        stockSymbol = set()        
        for str in str:
            df = pd.DataFrame()
            for col in ['province','city','chairman','manager','secretary','introduction','main_business','business_scope', 'holder']:
                df = pd.concat([df, self.data[self.data[col].str.find(str) != -1]])
            if len(stockSymbol) != 0:
                stockSymbol = stockSymbol & set(df.index)
            else:
                stockSymbol = set(df.index)
        df = self.data.loc[stockSymbol]
        return df



class tushareHolder():
    def __init__(self):
        self.stockListPath = r'f:\\stockpro\\stockData\\stockList.pkl'
        self.stockList = self.loadStockList()
        self.dataPath = r'f:\\stockpro\\stockData\\tushareHolder.pkl'
        self.data = self.readData()
    
    def loadStockList(self):
        try:
            with open(self.stockListPath,'rb') as fs:
                l = pickle.load(fs)
        except:
            print("读取股票列表失败")
        else:
            return l 
    
    def readData(self):
        try:
            with open(self.dataPath,'rb') as fs:
                d = pickle.load(fs)
            return d       
        except:
            logger.info("打开 tushareBasicData 错误，新建数据")
            return self.updateHolderData(self.stockList)      
    
    def save(self):
        try:
            with open(self.dataPath,'wb') as fs:
                pickle.dump(self.data, fs, pickle.HIGHEST_PROTOCOL)
        except:
            logger.info("存储 tushareBasicData 失败")
        else:
            logger.info("存储 tushareBasicData 成功")
    
    def updateHolderData(self, stockList):
        import time
        holderdict = {}
        for k in stockList:    
            df = pro.top10_holders(ts_code = k, start_date='20200101')
            df.drop_duplicates(inplace = True)
            df = df[:10]
            _dict = self.hanlderData(df)
            holderdict.update({k : _dict})
            time.sleep(6)
        return holderdict

    def hanlderData(self,v):
        date = v['end_date'][0]
        key = v['ts_code'][0]
        try:
            v.drop(['ts_code','ann_date','end_date'], axis = 1, inplace = True)
            v.sort_values('hold_ratio', ascending=False, inplace = True)
            v.index = range(len(v))
        except:
            print(f"处理失败：{key}")      
        finally:
            return {date : v}




# name_value = list(holder.data[k][d]['holder_name'].values)
# ratio_value = list(holder.data[k][d]['hold_ratio'].values)
# def createHolderColumns(self):
#     holderDF = pd.DataFrame(data = [str(dict(zip(holder.data[k][d]['holder_name'].values, holder.data[k][d]['hold_ratio'].values))) for k in holder.data], index =  [k for k in holder.data ], columns = ['holder'])

