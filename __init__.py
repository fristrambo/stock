# package stock.py

import pdb
from stock import data
from stock import foundation
from stock import capital
from stock import query
from stock import tradeDate
from stock import kPicture
from stock import basicData
from stock import windows

import threading


import os
if os.getcwd !="f:\\stockpro" : os.chdir("f:\\stockpro\\")

import sys
import pandas as pd
import numpy as np
import datetime


class Thread(threading.Thread):
    def __init__(self, func, args=()):
        super(Thread, self).__init__()
        self.func = func
        self.args = args
    
    def run(self):
        self.result = self.func(*self.args)
    
    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None  

class Stock():
    def __init__(self):
        self.s = data.NewData()
        self.f = foundation.Foundation()
        self.c = capital.Capital()
        self.b = basicData.tushareBasicData()
        self.h = basicData.tushareHolder()
        self.q = query.queryData(self.s, self.f, self.c)
        self.td = tradeDate.tradeDate()
        self.w = windows.main

    def save(self):
        self.s.save()
        self.f.save()
        self.c.save()
        self.q.save()

    def update(self):
        self.s.update()
        self.c.update()
        self.q.update()

    def windows(self):
        self.windowsThread = threading.Thread(target = self.w, args = (self, ))
        self.windowsThread.start()

st = Stock()                # 创建 Stock 实例
