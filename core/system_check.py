import os
import platform
import subprocess
from core.logger import Logger

class SystemCheck:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
    
    def check_linux_system(self):
        """
        检查是否为Linux系统
        """
        self.logger.info("检查操作系统类型...")
        system = platform.system()
        if system != "Linux":
            self.logger.error(f"当前系统为 {system}，仅支持Linux系统")
            print("错误：仅支持Linux系统，当前系统为", system)
            exit(1)
        self.logger.info(f"当前系统为 {system}，继续执行")
        return True
    
    def check_root_privilege(self):
        """
        检查是否具有root权限
        """
        self.logger.info("检查root权限...")
        if os.geteuid() != 0:
            self.logger.error("需要root权限执行此脚本")
            print("错误：需要root权限执行此脚本，请使用sudo或root用户运行")
            exit(1)
        self.logger.info("已获取root权限，继续执行")
        return True
    
    def check_required_commands(self, commands=None):
        """
        检查必要命令是否存在
        
        Args:
            commands: 需要检查的命令列表，默认为["curl", "wget", "ssh", "scp"]
        """
        if commands is None:
            commands = ["curl", "wget", "ssh", "scp"]
        
        self.logger.info("检查必要命令是否存在...")
        missing_commands = []
        
        for cmd in commands:
            try:
                subprocess.run([cmd, "--version"], capture_output=True, check=True)
                self.logger.info(f"命令 {cmd} 已安装")
            except subprocess.CalledProcessError:
                # 命令存在但返回非零退出码，可能是--version参数不支持
                pass
            except FileNotFoundError:
                self.logger.error(f"命令 {cmd} 未安装")
                missing_commands.append(cmd)
        
        if missing_commands:
            self.logger.error(f"缺少必要命令：{missing_commands}")
            print(f"错误：缺少必要命令，请先安装：{missing_commands}")
            exit(1)
        
        self.logger.info("所有必要命令已安装，继续执行")
        return True
    
    def run_all_checks(self):
        """
        运行所有系统检查
        """
        self.logger.info("开始系统检查...")
        
        self.check_linux_system()
        self.check_root_privilege()
        self.check_required_commands()
        
        self.logger.info("系统检查完成，所有条件均满足")
        return True
