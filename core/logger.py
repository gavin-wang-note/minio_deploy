import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler

class CustomFormatter(logging.Formatter):
    """
    自定义日志格式，将微秒转换为毫秒
    """
    def formatTime(self, record, datefmt=None):
        if datefmt:
            # 使用自定义格式
            dt = time.strftime(datefmt, time.localtime(record.created))
            # 添加毫秒（3位）
            ms = int(record.msecs)
            return f"{dt},{ms:03d}"
        else:
            # 使用默认格式
            return super().formatTime(record, datefmt)

class Logger:
    def __init__(self, log_file="logs/minio-deploy.log", log_level=logging.DEBUG):
        self.log_file = log_file
        self.log_level = log_level
        self.logger = logging.getLogger("minio-deploy")
        self.logger.setLevel(log_level)
        self._setup_logger()
    
    def _setup_logger(self):
        # 确保日志目录存在
        log_dir = os.path.dirname(self.log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 清除已有的处理器
        if self.logger.handlers:
            for handler in self.logger.handlers:
                self.logger.removeHandler(handler)
        
        # 定义日志格式：[年-月-日 时:分:秒,毫秒]-[日志级别]-[xx.py:行号] 具体日志内容
        formatter = CustomFormatter(
            "[%(asctime)s]-[%(levelname)s]-[%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # 文件处理器：按日期滚动
        file_handler = TimedRotatingFileHandler(
            self.log_file,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(self.log_level)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(self.log_level)
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        return self.logger
    
    def set_level(self, level):
        self.log_level = level
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
