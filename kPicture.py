# kPicture.py
import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

# dat = data.NewData()  指标数据

# ddat = dat.ddata['000001.SZ'].iloc[-100:][['open','high','low','close','vol','ma10','ma20','ma30']]
# ddat['t'] = range(1, ddat.shape[0] + 1)
# wdat = dat.wdata['000001.SZ'].iloc[-100:][['open','high','low','close','vol','ma10','ma20','ma30']]
# wdat['t'] = range(1, wdat.shape[0] + 1)
# mdat = dat.mdata['000001.SZ'].iloc[-100:][['open','high','low','close','vol','ma10','ma20','ma30']]
# mdat['t'] = range(1, mdat.shape[0] + 1)


## Create a subclass of GraphicsObject.
## The only required methods are paint() and boundingRect() 
## (see QGraphicsItem documentation)

class kDialog(QtGui.QDialog):
    def __init__(self, data, parent = None):
        super(kDialog, self).__init__(parent)
        typedict ={ 'd':'日线', 'w':'周线', 'm':'月线'}
        self.data = data()
        self.setWindowTitle(f'{self.data["name"]}{typedict[self.data["type"]]}')
        self.resize(800, 600)
        self.createPlot

    def createPlot(self):         
        self.item = CandlestickItem(self.data['data'])    # 建立 k 线图实例
        self.vlayout = QtGui.QVBoxLayout(self)
        self.plt = pg.plot(self.vlayout)
        self.plt.addItem(self.item)
        self.plt.plot(self.data['data']['ma30'], pen = 'r')
        self.plt.plot(self.data['data']['ma20'], pen = 'b')
        self.plt.plot(self.data['data']['ma10'], pen = 'y')
        self.vlayout.addWidget(self.item)    


class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  ## data must have fields: time, open, close, min, max
        self.generatePicture()
    
    def generatePicture(self):
        ## pre-computing a QPicture object allows paint() to run much more quickly, 
        ## rather than re-drawing the shapes every time.
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        p.setPen(pg.mkPen('w'))
        w = 1 / 3.
        for (open, max, min, close, vol, ma10, ma20, ma30, t) in self.data.values:
            p.drawLine(QtCore.QPointF(t, min), QtCore.QPointF(t, max))
            if open > close:
                p.setBrush(pg.mkBrush('g'))
            else:
                p.setBrush(pg.mkBrush('r'))
            p.drawRect(QtCore.QRectF(t-w, open, w*2, close-open))
        p.end()
    
    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        ## (in this case, QPicture does all the work of computing the bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())

def showKline(data):
    # print(data['code'], data['name'], data['type'], data['data'])
    item = CandlestickItem(data['data'])    # 建立 k 线图实例
    typedict ={ 'd':'日线', 'w':'周线', 'm':'月线'} 
    plt = pg.plot()
    plt.addItem(item)
    plt.plot(data['data']['ma30'], pen = 'r')
    plt.plot(data['data']['ma20'], pen = 'b')
    plt.plot(data['data']['ma10'], pen = 'y')    
    plt.setWindowTitle(f'{data["name"]}{typedict[data["type"]]}')

## Start Qt event loop unless running in interactive mode or using pyside.
if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()