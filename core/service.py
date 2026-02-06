import os
import subprocess
from core.logger import Logger

class ServiceManager:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
        self.service_name = "minio"
        self.service_file = f"/etc/systemd/system/{self.service_name}.service"
    
    def create_service_file(self, data_dir, listen_port=9000, console_port=9001, credentials=None, erasure_coding=None):
        """
        创建MinIO systemd服务文件
        
        Args:
            data_dir: 数据目录
            listen_port: 监听端口，默认为9000
            console_port: 控制台端口，默认为9001
            credentials: 认证信息，包含root_user和root_password
            erasure_coding: 纠删码配置
        
        Returns:
            bool: True表示创建成功，False表示失败
        """
        self.logger.info(f"创建MinIO systemd服务文件：{self.service_file}")
        
        # 确保数据目录存在
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)
                self.logger.info(f"创建数据目录：{data_dir}")
            except Exception as e:
                self.logger.error(f"创建数据目录失败：{data_dir}，错误：{e}")
                return False
        
        # 构建服务文件内容
        service_content = f"""[Unit]
Description=MinIO Object Storage Service
Documentation=https://docs.min.io
Wants=network-online.target
After=network-online.target
AssertFileIsExecutable=/usr/local/bin/minio

[Service]
WorkingDirectory=/usr/local/bin

User=root
Group=root

EnvironmentFile=-/etc/default/minio
ExecStartPre=/bin/bash -c "[ -n \"$MINIO_VOLUMES\" ] || echo \"Variable MINIO_VOLUMES not set in /etc/default/minio\""

ExecStart=/usr/local/bin/minio server \
"
        
        # 添加纠删码配置（如果有）
        if erasure_coding is not None and erasure_coding.get("standard"):
            service_content += f"  --erasure-coding {erasure_coding['standard']} \
"
        
        # 添加监听端口和控制台端口
        service_content += f"  --address :{listen_port} \
  --console-address :{console_port} \
"
        
        # 添加数据目录
        service_content += f"  {data_dir}

# Let systemd restart this service always
Restart=always

# Specifies the maximum file descriptor number that can be opened by this process
LimitNOFILE=65536

# Specifies the maximum number of processes that can be created by this process
LimitNPROC=16384

# Time to wait before forcefully killing the process
TimeoutStopSec=5
SendSIGKILL=no

[Install]
WantedBy=multi-user.target
"""
        
        # 创建环境变量文件
        env_file = "/etc/default/minio"
        env_content = "# MinIO environment variables\n"
        
        if credentials:
            env_content += f"MINIO_ROOT_USER={credentials['root_user']}\n"
            env_content += f"MINIO_ROOT_PASSWORD={credentials['root_password']}\n"
        
        env_content += f"MINIO_VOLUMES=\"{data_dir}\"\n"
        env_content += f"MINIO_OPTS=\"--address :{listen_port} --console-address :{console_port}\"\n"
        
        if erasure_coding is not None and erasure_coding.get("standard"):
            env_content += f"MINIO_OPTS=\"$MINIO_OPTS --erasure-coding {erasure_coding['standard']}\"\n"
        
        try:
            # 写入环境变量文件
            with open(env_file, 'w') as f:
                f.write(env_content)
            self.logger.info(f"创建环境变量文件：{env_file}")
            
            # 写入服务文件
            with open(self.service_file, 'w') as f:
                f.write(service_content)
            self.logger.info(f"创建服务文件：{self.service_file}")
            
            # 重新加载systemd配置
            subprocess.run(["systemctl", "daemon-reload"], check=True, capture_output=True, text=True)
            self.logger.info("重新加载systemd配置")
            
            return True
        
        except Exception as e:
            self.logger.error(f"创建服务文件失败：{e}")
            return False
    
    def start_service(self):
        """
        启动MinIO服务
        
        Returns:
            bool: True表示启动成功，False表示失败
        """
        self.logger.info(f"启动MinIO服务：{self.service_name}")
        
        try:
            subprocess.run(["systemctl", "start", self.service_name], check=True, capture_output=True, text=True)
            self.logger.info(f"MinIO服务启动成功：{self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"MinIO服务启动失败：{e}")
            return False
    
    def stop_service(self):
        """
        停止MinIO服务
        
        Returns:
            bool: True表示停止成功，False表示失败
        """
        self.logger.info(f"停止MinIO服务：{self.service_name}")
        
        try:
            subprocess.run(["systemctl", "stop", self.service_name], check=True, capture_output=True, text=True)
            self.logger.info(f"MinIO服务停止成功：{self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"MinIO服务停止失败：{e}")
            return False
    
    def restart_service(self):
        """
        重启MinIO服务
        
        Returns:
            bool: True表示重启成功，False表示失败
        """
        self.logger.info(f"重启MinIO服务：{self.service_name}")
        
        try:
            subprocess.run(["systemctl", "restart", self.service_name], check=True, capture_output=True, text=True)
            self.logger.info(f"MinIO服务重启成功：{self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"MinIO服务重启失败：{e}")
            return False
    
    def enable_service(self):
        """
        设置MinIO服务开机自启
        
        Returns:
            bool: True表示设置成功，False表示失败
        """
        self.logger.info(f"设置MinIO服务开机自启：{self.service_name}")
        
        try:
            subprocess.run(["systemctl", "enable", self.service_name], check=True, capture_output=True, text=True)
            self.logger.info(f"MinIO服务开机自启设置成功：{self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"MinIO服务开机自启设置失败：{e}")
            return False
    
    def disable_service(self):
        """
        禁用MinIO服务开机自启
        
        Returns:
            bool: True表示禁用成功，False表示失败
        """
        self.logger.info(f"禁用MinIO服务开机自启：{self.service_name}")
        
        try:
            subprocess.run(["systemctl", "disable", self.service_name], check=True, capture_output=True, text=True)
            self.logger.info(f"MinIO服务开机自启禁用成功：{self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"MinIO服务开机自启禁用失败：{e}")
            return False
    
    def check_service_exists(self):
        """
        检查MinIO服务是否存在
        
        Returns:
            bool: True表示服务存在，False表示服务不存在
        """
        self.logger.info(f"检查MinIO服务是否存在：{self.service_name}")
        
        try:
            # 检查服务文件是否存在
            if os.path.exists(self.service_file):
                self.logger.info(f"MinIO服务文件存在：{self.service_file}")
                return True
            
            # 尝试通过systemctl检查服务是否存在
            result = subprocess.run(
                ["systemctl", "list-unit-files", "--type", "service", "|", "grep", "minio"],
                shell=True, capture_output=True, text=True, check=True
            )
            
            if self.service_name in result.stdout:
                self.logger.info(f"MinIO服务存在：{self.service_name}")
                return True
            else:
                self.logger.info(f"MinIO服务不存在：{self.service_name}")
                return False
                
        except Exception as e:
            # 如果命令执行失败，可能是因为服务不存在
            self.logger.debug(f"检查MinIO服务是否存在时出错：{e}")
            self.logger.info(f"MinIO服务不存在：{self.service_name}")
            return False
    
    def check_service_status(self):
        """
        检查MinIO服务状态
        
        Returns:
            tuple: (status, output)，status为True表示服务运行正常，output为服务状态输出
        """
        self.logger.info(f"检查MinIO服务状态：{self.service_name}")
        
        try:
            result = subprocess.run(
                ["systemctl", "status", self.service_name],
                capture_output=True, text=True, check=True
            )
            output = result.stdout
            
            # 检查服务是否运行
            if "active (running)" in output:
                self.logger.info(f"MinIO服务运行正常：{self.service_name}")
                return (True, output)
            else:
                self.logger.warning(f"MinIO服务未运行：{self.service_name}")
                return (False, output)
                
        except Exception as e:
            self.logger.error(f"检查MinIO服务状态失败：{e}")
            return (False, str(e))
    
    def get_service_logs(self, lines=50):
        """
        获取MinIO服务日志
        
        Args:
            lines: 日志行数，默认为50
        
        Returns:
            str: 日志内容
        """
        self.logger.info(f"获取MinIO服务日志，行数：{lines}")
        
        try:
            result = subprocess.run(
                ["journalctl", "-u", self.service_name, "-n", str(lines), "--no-pager"],
                capture_output=True, text=True, check=True
            )
            return result.stdout
        except Exception as e:
            self.logger.error(f"获取MinIO服务日志失败：{e}")
            return f"获取日志失败：{e}"
    
    def configure_service(self, data_dir, listen_port=9000, console_port=9001, credentials=None, erasure_coding=None):
        """
        配置MinIO服务：创建服务文件、设置开机自启、启动服务
        
        Args:
            data_dir: 数据目录
            listen_port: 监听端口，默认为9000
            console_port: 控制台端口，默认为9001
            credentials: 认证信息
            erasure_coding: 纠删码配置
        
        Returns:
            bool: True表示配置成功，False表示失败
        """
        self.logger.info("开始配置MinIO服务")
        
        # 创建服务文件
        if not self.create_service_file(data_dir, listen_port, console_port, credentials, erasure_coding):
            return False
        
        # 设置开机自启
        if not self.enable_service():
            return False
        
        # 启动服务
        if not self.start_service():
            return False
        
        # 检查服务状态
        status, output = self.check_service_status()
        if not status:
            self.logger.error("MinIO服务启动后状态异常")
            self.logger.error(f"服务状态输出：{output}")
            self.logger.error(f"服务日志：{self.get_service_logs()}")
            return False
        
        self.logger.info("MinIO服务配置成功")
        return True
