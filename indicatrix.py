# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 17:36:38 2019

@author: frist
"""

import numpy as np
import pandas as pd
import time 
import copy
'''
if 'Linux' in platform.platform():
    file_path='/home/zhangpei/stock/backup'
if 'Windows' in platform.platform():
    file_path='e:\\stock\\backup\\'
'''
#=====================================================================================================
import stock.logger
stock_logger = stock.logger.StockLogging()

#======================================================================================================    
#运行时间装饰函数
def timeit(_f):
    def wrapper(*args):
        starttime = time.perf_counter()
        _f(args[0])
        endtime = time.perf_counter()
        print(endtime-starttime)
    return wrapper

#======================================================================================================    
#运行状态装饰函数
def logit(_f):
    def wrapper(*args):
        starttime = time.perf_counter()
        _f(args[0])
        endtime = time.perf_counter()
        # try:
        #     indicatrix_logger.info(f"{_f.__name__} code={args[1]} length={len(args[0])} start={args[0].index[0]}  end={args[0].index[-1]} runtime:{endtime-starttime:.4f}")
        # except Exception as e:
        #     indicatrix_logger.info(f"{_f.__name__} code={args[1]} {e}")
    return wrapper

#======================================================================================================    
def setIndicatrix(_df):
    #为DataFrame设置指标 _df 为传入DataFrame    
    _df['ma5'], _df['ma10'], _df['ma20'], _df['ma30'] = [0] * 4                      
    _df['diff'], _df['dea'], _df['macd'] = [0] * 3
    _df['ad'], _df['wad'], _df['mawad'] = [0] * 3
    _df['boll_up'], _df['boll_dn'] = np.nan, np.nan 
    _df['red_three'] = np.nan
    
    ma(_df)
    wad(_df)
    macd(_df)
    boll(_df)
    stat_ma(_df)
    stat_macd(_df)
    stat_wad(_df)
    stat_dea(_df)
    stat_mawad(_df)
    red_three(_df)
    # std_ma250(_df)

#======================================================================================================    
@logit
def ma(_df):    
    _df.sort_index(ascending=True,inplace=True)
    _df['ma5'], _df['ma10'], _df['ma20'], _df['ma30'] = _df['close'].rolling(5).mean(), _df['close'].rolling(10).mean(), _df['close'].rolling(20).mean(), _df['close'].rolling(30).mean()

#====================================================================================================== 
@logit   
def boll(_df):
    # boll 指标函数
    # df 为传入 DataFrame，k 为股票代码
    #'boll_up':布尔上轨线，'boll_dn':布尔下轨线
    _df.sort_index(ascending=True,inplace=True)
    tmp=_df['close'].rolling(20).std()
    _df['boll_up']=_df['ma20']+2*tmp
    _df['boll_dn']=_df['ma20']-2*tmp

#======================================================================================================    
@logit
def macd(_df,short_=12,long_=26,m=9):
    '''
    data是包含高开低收成交量的标准dataframe
    short_,long_,m分别是macd的三个参数
    返回值是包含原始数据和diff,dea,macd三个列的dataframe
    '''
    _df['diff']=_df['close'].ewm(adjust=False,alpha=2/(short_+1),ignore_na=True).mean()-\
                _df['close'].ewm(adjust=False,alpha=2/(long_+1),ignore_na=True).mean()
    _df['dea']=_df['diff'].ewm(adjust=False,alpha=2/(m+1),ignore_na=True).mean()
    _df['macd']=2*(_df['diff']-_df['dea'])

#======================================================================================================    
@logit
def wad(_df):
    _df.sort_index(ascending=True,inplace=True)
    TRL=_df['close'].shift(1).mask(_df['close'].shift(1)>_df['low'],_df['low'])
    TRH=_df['close'].shift(1).mask(_df['close'].shift(1)<_df['high'],_df['high'])
    _df['ad']=_df['ad'].mask(_df['close']<_df['close'].shift(1),_df['close']-TRH)
    _df['ad']=_df['ad'].mask(_df['close']>_df['close'].shift(1),_df['close']-TRL)
    _df['ad']=_df['ad'].mask(_df['close']==_df['close'].shift(1),0)
    _df['wad']=_df['ad'].cumsum()
    _df['mawad'] = _df['wad'].rolling(30).mean()

#======================================================================================================    
# @logit
# def K_line(_db):                #设置当日指标
#     _db['line'],_db['open_s']=np.nan,np.nan
#     _db['line'].mask(_db.eval('p_change<-9.7 and high==low'),'一字线跌停',inplace=True)
#     _db['line'].mask(_db.eval('p_change>9.9 and high==low'),'一字线涨停',inplace=True)
#     _db['open_s'].mask(_db.eval('open/(close/(100+p_change)*100)-1>0.097'),'涨停开盘',inplace=True)
#     _db['open_s'].mask(_db.eval('0.05<open/(close/(100+p_change)*100)-1<0.097'),'大幅高开',inplace=True)
#     _db['open_s'].mask(_db.eval('0<open/(close/(100+p_change)*100)-1<0.05'),'小幅高开',inplace=True)
#     _db['open_s'].mask(_db.eval('open/(close/(100+p_change)*100)-1<-0.099'),'跌停开盘',inplace=True)
#     _db['open_s'].mask(_db.eval('-0.05<open/(close/(100+p_change)*100)-1<0'),'小幅低开',inplace=True)
#     _db['open_s'].mask(_db.eval('-0.099<open/(close/(100+p_change)*100)-1<-0.05'),'大幅低开',inplace=True)
#     _db['line'].mask(_db.eval('(close-open)/open<-0.07'),'大阴线',inplace=True)
#     _db['line'].mask(_db.eval('-0.07<(close-open)/open<-0.03'),'中阴线',inplace=True)
#     _db['line'].mask(_db.eval('-0.03<(close-open)/open<0'),'小阴线',inplace=True)
#     _db['line'].mask(_db.eval('0<(close-open)/open<0.03'),'小阳线',inplace=True)
#     _db['line'].mask(_db.eval('0.03<(close-open)/open<0.07'),'中阳线',inplace=True)
#     _db['line'].mask(_db.eval('0.07<(close-open)/open'),'大阳线',inplace=True)
#     _db['line'].mask(_db.eval('open==close and high!=low'),'十字星',inplace=True)

#======================================================================================================    
@logit
def red_three(df,n = None):
    df['red_line'], df['red_count'] = np.nan, np.nan
    df['red_line'] = df['close'] >= df['open']      #.where(df['close']/df['open'] -1 < 0.03)
    red_line = copy.copy(df['red_line'])
    for i in range(2, 10):
        red_line.mask(~(red_line & red_line.shift(1)),False,inplace=True)
        df['red_count'].mask(red_line,i,inplace=True)


def macd_count(df, n = None):
    if isinstance(df, pd.DataFrame) and len(df) > 0:
        df['macd_count'] = np.nan
        macd = copy.copy(df['macd'])
        macd = macd.map(lambda x: x > 0 )
        for i in range(2,10):
            macd.mask(~(macd & macd.shift(1)),False,inplace=True)
            df['macd_count'].mask(macd,i,inplace=True)


#======================================================================================================    
@logit
def stat_ma(df):
    df['stat_ma']=np.nan
    ma5_up=(df.close>df.ma5) & (df.close.shift(1)<df.ma5.shift(1))
    ma10_up=(df.close>df.ma10) & (df.close.shift(1)<df.ma10.shift(1))
    ma20_up=(df.close>df.ma20) & (df.close.shift(1)<df.ma20.shift(1))
    
    ma5_down=(df.close<df.ma5) & (df.close.shift(1)>df.ma5.shift(1))
    ma10_down=(df.close<df.ma10) & (df.close.shift(1)>df.ma10.shift(1))
    ma20_down=(df.close<df.ma20) & (df.close.shift(1)>df.ma20.shift(1))
    df['stat_ma']=df['stat_ma'].mask(ma5_up,'突破5日均线').mask(ma10_up,'突破10日均线').mask(ma20_up,'突破20日均线') \
                .mask(ma5_down,'跌破5日均线').mask(ma10_down,'跌破10日均线').mask(ma20_down,'跌破20日均线')
    
    ma5_win=(df.ma5>df.ma10) & (df.ma5.shift(1)<df.ma10.shift(1))
    ma10_win=(df.ma10>df.ma20) & (df.ma10.shift(1)<df.ma20.shift(1))
    ma20_win=(df.ma20>df.ma30) & (df.ma20.shift(1)<df.ma30.shift(1))
    
    ma5_lost=(df.ma5<df.ma10) & (df.ma5.shift(1)>df.ma10.shift(1))
    ma10_lost=(df.ma10<df.ma20) & (df.ma10.shift(1)>df.ma20.shift(1))
    ma20_lost=(df.ma20<df.ma30) & (df.ma20.shift(1)>df.ma30.shift(1))
    
    df['stat_ma']=df['stat_ma'].mask(ma5_win,'5日均线金叉').mask(ma10_win,'10日均线金叉').mask(ma20_win,'20日均线金叉') \
                .mask(ma5_lost,'5日均线死叉').mask(ma10_lost,'10日均线死叉').mask(ma20_lost,'20日均线死叉')

#======================================================================================================    
@logit
def stat_wad(df):
    df['stat_wad']=np.nan
    wad_up=(df['mawad']>df['mawad'].shift(1)) & (df['wad']>df['mawad'])
    wad_down=(df['mawad']<df['mawad'].shift(1)) & (df['wad']<df['mawad'])
    wad_win=(df['wad']>df['mawad']) & (df['wad'].shift(1)<df['mawad'].shift(1))
    wad_lost=(df['wad']<df['mawad']) & (df['wad'].shift(1)>df['mawad'].shift(1))
    df['stat_wad']=df['stat_wad'].mask(wad_up,'wad上升趋势').mask(wad_down,'wad下降趋势') \
                .mask(wad_win,'wad金叉').mask(wad_lost,'wad死叉')
                   
#======================================================================================================    
@logit
def stat_macd(df):
    df['stat_macd']=np.nan
    macd_up=(df['diff']>df['dea']) & (df['diff']>df['diff'].shift(1))
    macd_down=(df['diff']<df['dea']) & (df['diff']<df['diff'].shift(1))
    macd_win=(df['diff']>df['dea']) & (df['diff'].shift(1)<df['dea'].shift(1))
    macd_lost=(df['diff']<df['dea']) & (df['diff'].shift(1)>df['dea'].shift(1))
    df['stat_macd']=df['stat_macd'].mask(macd_up,'macd上升趋势').mask(macd_down,'macd下降趋势') \
                .mask(macd_win,'macd金叉').mask(macd_lost,'macd死叉') 

#======================================================================================================    
@logit
def std_ma30(_df):
    _df['std_ma250']=np.nan
    _df['std_ma250']=np.std(_df[['ma5','ma10','ma20','ma30']], axis=1)

#======================================================================================================    
@logit
def drawdown_30(_df):
    _df['drawdown_30']=np.nan
    _df['drawdown_30']=(_df['close']-_df['low'].rolling(30).min())/(_df['high'].rolling(30).max()-_df['low'].rolling(30).min())

#======================================================================================================    
@logit
def stat_mawad(_df):
    _df['stat_mawad']=np.nan
    _df['stat_mawad'] = _df['mawad'].diff(1)

#======================================================================================================    
@logit
def stat_dea(_df):
    _df['stat_dea']=np.nan
    _df['stat_dea'] = _df['dea'].diff(1)
   