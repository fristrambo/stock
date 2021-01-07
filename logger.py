# logger.py
import logging

class StockLogging():
    # 初始化日志
    def __init__(self):
        self.logPath = r'f:\\stockpro\\logging\\'
        self.logName = 'stock.log'
        self.logFile = self.logPath + self.logName
        # 日志的输出格式
        logging.basicConfig(
            level = logging.INFO,  # 级别：CRITICAL > ERROR > WARNING > INFO > DEBUG，默认级别为 WARNING
            format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt = '%Y-%m-%d %H:%M:%S',
            filename = self.logFile,
            filemode='a')
    
    def info(self, content):
        logging.info(content)

    def debug(self, content):
        logging.debug(content)
        # 可以写其他的函数,使用其他级别的log  
          
    def error(self,content):
        logging.error(content)


# def stockLogger():    
#     #创建记录器
#     global loadstock_logger
#     loadstock_logger = logging.getLogger('loadstock_logger') 
#     loadstock_logger.setLevel(10)
#     #创建处理器
#     fh = logging.FileHandler('loadstock.log') 
#     fh.setLevel(10)
#     ch = logging.StreamHandler()
#     ch.setLevel(10)         
#     #创建格式器
#     loadstock_logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     #为处理器添加格式器
#     fh.setFormatter(loadstock_logger_formatter) 
#     ch.setFormatter(loadstock_logger_formatter) 
#     #为记录器添加处理器
#     loadstock_logger.addHandler(fh) 
#     loadstock_logger.addHandler(ch)
    
