# windows.py 

import ctypes    #需要用到的库
ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('myappid')

import pandas as pd
import numpy as np
import os
import sys
import json
import pickle
from PyQt5.Qt import *
from PyQt5.QtWidgets import (QMainWindow, QTableView, QWidget, QApplication, QVBoxLayout, QAbstractItemView,  
                            QTableWidget, QTableWidgetItem, QLabel, QPushButton, QGridLayout, 
                            QSpacerItem,  QSizePolicy, QMenuBar, QStatusBar, QDialog, QTextEdit, QLineEdit, QComboBox, QHeaderView) 
from PyQt5.QtGui import (QStandardItem, QStandardItemModel, QIcon, QPicture, QPainter)
from PyQt5.QtCore import (QVariant, QThread, pyqtSignal, QRect, QAbstractTableModel, QRectF, QPointF)
import pyqtgraph as pg

iconPath = r'.\icon\favicon.ico'


class table(QTableView):
    def __init__(self, stock, _df):
        super(table, self).__init__()
        self.stock = stock
        self.model = dataModel(_df)
        self.hh = self.horizontalHeader()
        self.hh.setVisible(True)
        self.vh = self.verticalHeader()
        self.vh.setVisible(True)
        self.setSortingEnabled(True)
        self.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.setAlternatingRowColors(True)        
        self.setModel(self.model)
        self.resizeColumnsToContents()
        self.clicked.connect(self.doubleClickedHandle)
        self.count = 0   

    def doubleClickedHandle(self, index):
        # print(self.model.headerData(index.row(), 2, 0))       # 根据索引获取 行标题    
        # print(self.model.headerData(index.column(), 1, 0))    # 根据索引获取 列标题
        # print(index.row(), index.column())                    # 索引行，列
        # print(index.data())                                   # 根据索引提取数据
        # print(self.model.index(index.row(), 0).data())        # 索引数据
        if index.column() in [10, 11]:
            self.showK(self.model.headerData(index.row(), 2, 0), self.model.index(index.row(), 0).data(), t = 'd')
        if index.column() in [12, 13]:
            self.showK(self.model.headerData(index.row(), 2, 0), self.model.index(index.row(), 0).data(), t = 'w')
        if index.column() in [14, 15]:
            self.showK(self.model.headerData(index.row(), 2, 0), self.model.index(index.row(), 0).data(), t = 'm')
        if index.column() in [2, 3]:
            self.showHolder(self.model.headerData(index.row(), 2, 0))

    def showHolder(self, code):
        # holderdata = holder.data['code']['20200930']
        showH = HDialog(self.stock, code, parent = self)
        showH.show()
        self.thread = RunThread(self.count)
        self.count += 1
        self.thread.start()

    def showK(self, code, name = '', t = 'd'):           
        typedict = {'d' : self.stock.s.ddata[code], 'w': self.stock.s.wdata[code], 'm' : self.stock.s.mdata[code] }
        columns = ['open','high','low','close','vol','ma10','ma20','ma30']
        df = typedict[t][columns].iloc[-200:]
        df['t'] = range(1, df.shape[0] + 1) 
        data = {'code' : code, 'name' : name, 'type': t, 'data' : df}
        # print(data['code'], data['name'], data['type'], data['data'])
        kplot = kDialog(data, parent = self)
        kplot.show()
        self.thread = RunThread(self.count)
        self.count += 1
        self.thread.start() 

class kDialog(QDialog):
    def __init__(self, data, parent = None):
        super(kDialog, self).__init__(parent)
        typedict ={ 'd':'日线', 'w':'周线', 'm':'月线'}
        self.data = data
        self.setWindowTitle(f'{self.data["name"]}{typedict[self.data["type"]]}')
        self.layout =  QVBoxLayout()
        self.setLayout(self.layout)
        self.resize(800, 600)
        self.createPlot()
    
    def createPlot(self):         
        self.item = CandlestickItem(self.data['data'])    # 建立 k 线图实例
        self.plt = pg.PlotWidget(parent = self)
        self.plt.plot(self.data['data']['ma30'], pen = 'r')
        self.plt.plot(self.data['data']['ma20'], pen = 'b')
        self.plt.plot(self.data['data']['ma10'], pen = 'y')
        self.plt.addItem(self.item)
        self.layout.addWidget(self.plt) 


class HDialog(QDialog):
    def __init__(self, stock, code, parent = None):
        super(HDialog, self).__init__(parent)
        self.stock = stock
        self.code = code
        self.data = self.stock.h.data[code]['20200930']
        self.setWindowTitle(f'[{self.code}] - 20200930 - 十大股东列表')
        self.table = QTableWidget(self)
        self.layout =  QVBoxLayout()
        self.layout.addWidget(self.table)
        self.setLayout(self.layout)
        self.resize(320, 400)
        self.creatTable()
    
    def creatTable(self):
        self.table.setRowCount(self.data.shape[0])
        self.table.setColumnCount(self.data.shape[1])
        self.table.setHorizontalHeaderLabels(['持股人名称', '持股数量', '持股比率'])     # 列标题
        self.table.setVerticalHeaderLabels(self.data.index.map(str))         # 行标题
        self.table.setSortingEnabled(True)                    # 排序
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)      # 不可编辑
        self.table.setAlternatingRowColors(True)              # 交替颜色
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.verticalHeader().setResizeMode(QHeaderView.Stretch)
        for row in range(self.table.rowCount()):                     # 按行设置单元格数据
            for columns in range(self.table.columnCount()):             
                item = QTableWidgetItem(str(self.data.iloc[-(row + 1), columns]))
                if columns == 1:
                    item = QTableWidgetItem(f"{str(round(self.data.iloc[-(row + 1), columns]/100000000, 2))} 亿股" )
                if columns == 2:
                    item = QTableWidgetItem(str(self.data.iloc[-(row + 1), columns]))
                self.table.setItem(row, columns, item)

class dataModel(QAbstractTableModel):
    def __init__(self, data, parent = None):
        super(dataModel, self).__init__()
        self.tabledata = data
    
    def rowCount(self, parent = None):
        return self.tabledata.shape[0]
    
    def columnCount(self, parent = None):
        return self.tabledata.shape[1]
    
    def data(self, index, role):
        if not index.isValid():
            return QVariant()
        elif role == 0:
            return str(self.tabledata.iloc[index.row(), index.column()])
        else:
            return QVariant()
        
    
    def headerData(self, section, orientation, role):
        if orientation == 1:
            if role == 0:
                if 0 <= section < self.tabledata.shape[1]:
                    return self.tabledata.columns[section]
                else:
                    return None
        if orientation == 2:
            if role == 0:
                if 0 <= section < self.tabledata.shape[0]:
                    return self.tabledata.index[section]
                else:
                    return None
    
    def sort(self, columns, order):
        self.beginResetModel()
        if order == Qt.AscendingOrder:
            self.tabledata.sort_values(self.tabledata.columns[columns], ascending = True, inplace = True)
        else:
            self.tabledata.sort_values(self.tabledata.columns[columns], ascending = False, inplace = True)
        self.endResetModel()

class MainWindow(QMainWindow):
    def __init__(self, stock, parent=None):
        super(MainWindow, self).__init__(parent)
        self.stock = stock
        self.setWindowIcon(QIcon(iconPath))
        self.setGeometry(200, 200, 800, 600)        # (x, y, width, height)
        self.fileMenu = self.menuBar().addMenu('文件')      # 添加菜单栏
        self.editMenu = self.menuBar().addMenu('编辑')
        self.viewMenu = self.menuBar().addMenu('视图')
        self.toolsMenu = self.menuBar().addMenu('工具')
        self.helpMenu = self.menuBar().addMenu('帮助')
        self.editToolBar = self.addToolBar('Edit')          # 添加工具栏
        self.viewToolBar = self.addToolBar("View")
        self.statusBar().showMessage("已准备好!")
        self.setWindowTitle('证券系统')
        self.setCentralWidget(queryWidget(self.stock))
 


class queryWidget(QWidget):
    def __init__(self, stock, parent = None):
        super(queryWidget, self).__init__(parent)
        self.stock = stock
        self.grid = QGridLayout()
        self.grid.setSpacing(10)        
        self.pushButton = QPushButton('查  询', self)
        self.selectdate = QComboBox(self)
        self.selectpolicy = QComboBox(self)
        [self.selectdate.addItem(k) for k in list(self.stock.q.queryData.keys())[::-1]]
        [self.selectpolicy.addItem(k) for k in ['dayredthree','weekredthree','monthredthree']]
        self.pushButton.clicked.connect(self.showTable)
        self.grid.addWidget(self.selectdate, 0, 0)
        self.grid.addWidget(self.selectpolicy, 1,0)
        self.grid.addWidget(self.pushButton, 0, 1)
        self.setLayout(self.grid)
        self.count = 0          

    def showTable(self):
        query = queryWindows(self.stock, self.stock.q.queryData[self.selectdate.currentText()][self.selectpolicy.currentText()], self.selectdate.currentText(), self.selectpolicy.currentText(), parent = self)
        query.show()
        self.thread = RunThread(self.count)
        self.count += 1
        self.thread.start()

class queryWindows(QDialog):
    def __init__(self, stock, df, date, policy, parent = None):
        super(queryWindows, self).__init__(parent)
        self.setWindowTitle(f"股票列表 - {date} - {policy}")
        self.resize(800, 600)
        self.table = table(stock, df)
        self.vlayout = QVBoxLayout(self)
        self.vlayout.addWidget(self.table)

class RunThread(QThread):
    def __init__(self, count):
        self.count = count
        super().__init__()
    def run(self):
        pass

  

class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  ## data must have fields: time, open, close, min, max
        self.generatePicture()
    
    def generatePicture(self):
        ## pre-computing a QPicture object allows paint() to run much more quickly, 
        ## rather than re-drawing the shapes every time.
        self.picture = QPicture()
        p = QPainter(self.picture)
        p.setPen(pg.mkPen('w'))
        w = 1 / 3.
        for (open, max, min, close, vol, ma10, ma20, ma30, t) in self.data.values:
            p.drawLine(QPointF(t, min), QPointF(t, max))
            if open > close:
                p.setBrush(pg.mkBrush('g'))
            else:
                p.setBrush(pg.mkBrush('r'))
            p.drawRect(QRectF(t-w, open, w*2, close-open))
        p.end()
    
    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        ## (in this case, QPicture does all the work of computing the bouning rect for us)
        return QRectF(self.picture.boundingRect())


def main(st):
    app = QApplication(sys.argv)
    mainWindow = MainWindow(st)
    mainWindow.show()
    sys.exit(app.exec_())
