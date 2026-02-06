import subprocess
import requests
import socket
from core.logger import Logger

class HealthChecker:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
    
    def check_port_listening(self, host, port, timeout=5):
        """
        检查指定端口是否监听
        
        Args:
            host: 主机IP或域名
            port: 端口号
            timeout: 超时时间，默认为5秒
        
        Returns:
            bool: True表示端口正在监听，False表示未监听
        """
        self.logger.info(f"检查端口 {host}:{port} 是否监听...")
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                result = s.connect_ex((host, port))
                if result == 0:
                    self.logger.info(f"端口 {host}:{port} 正在监听")
                    return True
                else:
                    self.logger.warning(f"端口 {host}:{port} 未监听")
                    return False
        except Exception as e:
            self.logger.error(f"检查端口 {host}:{port} 失败：{e}")
            return False
    
    def check_service_running(self, service_name="minio"):
        """
        检查MinIO服务是否运行
        
        Args:
            service_name: 服务名称，默认为minio
        
        Returns:
            bool: True表示服务正在运行，False表示未运行
        """
        self.logger.info(f"检查服务 {service_name} 是否运行...")
        
        try:
            result = subprocess.run(
                ["systemctl", "is-active", service_name],
                capture_output=True, text=True, check=True
            )
            status = result.stdout.strip()
            if status == "active":
                self.logger.info(f"服务 {service_name} 正在运行")
                return True
            else:
                self.logger.warning(f"服务 {service_name} 未运行，状态：{status}")
                return False
        except Exception as e:
            self.logger.error(f"检查服务 {service_name} 状态失败：{e}")
            return False
    
    def check_health_api(self, host, port=9000, secure=False, credentials=None, timeout=5):
        """
        调用MinIO健康检查API
        
        Args:
            host: 主机IP或域名
            port: 端口号，默认为9000
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息，包含root_user和root_password
            timeout: 超时时间，默认为5秒
        
        Returns:
            tuple: (status, response)，status为True表示健康检查通过，response为响应内容
        """
        protocol = "https" if secure else "http"
        url = f"{protocol}://{host}:{port}/minio/health/live"
        self.logger.info(f"调用健康检查API：{url}")
        
        try:
            # 添加认证信息（如果有）
            auth = None
            if credentials:
                auth = (credentials['root_user'], credentials['root_password'])
            
            response = requests.get(url, auth=auth, timeout=timeout, verify=False)  # verify=False用于自签名证书
            
            if response.status_code == 200:
                self.logger.info(f"健康检查API调用成功，状态码：{response.status_code}")
                return (True, response.text)
            else:
                self.logger.warning(f"健康检查API调用失败，状态码：{response.status_code}，响应：{response.text}")
                return (False, response.text)
                
        except Exception as e:
            self.logger.error(f"健康检查API调用失败：{e}")
            return (False, str(e))
    
    def check_mc_command(self):
        """
        检查mc命令是否可用
        
        Returns:
            bool: True表示mc命令可用，False表示不可用
        """
        self.logger.info("检查mc命令是否可用...")
        
        try:
            result = subprocess.run(
                ["mc", "--version"],
                capture_output=True, text=True, check=True
            )
            self.logger.info(f"mc命令可用，版本：{result.stdout.strip()}")
            return True
        except Exception as e:
            self.logger.warning(f"mc命令不可用：{e}")
            return False
    
    def check_bucket_access(self, host, port=9000, secure=False, credentials=None, bucket_name="test-bucket"):
        """
        测试存储桶访问
        
        Args:
            host: 主机IP或域名
            port: 端口号，默认为9000
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息，包含root_user和root_password
            bucket_name: 测试存储桶名称，默认为test-bucket
        
        Returns:
            tuple: (status, message)，status为True表示存储桶访问成功，message为详细信息
        """
        self.logger.info(f"测试存储桶访问：{bucket_name}")
        
        # 检查mc命令是否可用
        if not self.check_mc_command():
            return (False, "mc命令不可用，无法测试存储桶访问")
        
        try:
            protocol = "https" if secure else "http"
            endpoint = f"{protocol}://{host}:{port}"
            
            # 配置mc别名
            alias_name = "minio-local"
            cmd = [
                "mc", "alias", "set", alias_name, endpoint,
                credentials['root_user'], credentials['root_password']
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.logger.info(f"配置mc别名成功：{alias_name}")
            
            # 测试列出存储桶
            cmd = ["mc", "ls", alias_name]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.logger.info(f"列出存储桶成功，输出：{result.stdout.strip()}")
            
            # 尝试创建和删除测试存储桶（可选）
            try:
                # 创建测试存储桶
                cmd = ["mc", "mb", f"{alias_name}/{bucket_name}"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    self.logger.info(f"创建测试存储桶 {bucket_name} 成功")
                    
                    # 删除测试存储桶
                    cmd = ["mc", "rb", f"{alias_name}/{bucket_name}", "--force"]
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    self.logger.info(f"删除测试存储桶 {bucket_name} 成功")
                else:
                    self.logger.warning(f"创建测试存储桶 {bucket_name} 失败，可能已存在：{result.stderr.strip()}")
            except Exception as e:
                self.logger.warning(f"测试存储桶操作失败：{e}")
            
            return (True, "存储桶访问测试成功")
            
        except Exception as e:
            self.logger.error(f"存储桶访问测试失败：{e}")
            return (False, f"存储桶访问测试失败：{e}")
    
    def run_comprehensive_check(self, host, port=9000, console_port=9001, secure=False, credentials=None):
        """
        运行综合健康检查
        
        Args:
            host: 主机IP或域名
            port: 服务端口，默认为9000
            console_port: 控制台端口，默认为9001
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息，包含root_user和root_password
        
        Returns:
            dict: 健康检查结果，包含各个检查项的状态和详细信息
        """
        self.logger.info("开始运行综合健康检查...")
        
        results = {
            "service_running": False,
            "service_running_detail": "",
            "port_listening": False,
            "port_listening_detail": "",
            "console_port_listening": False,
            "console_port_listening_detail": "",
            "health_api": False,
            "health_api_detail": "",
            "mc_available": False,
            "mc_available_detail": "",
            "bucket_access": False,
            "bucket_access_detail": "",
            "overall_status": False
        }
        
        # 1. 检查服务是否运行
        results["service_running"] = self.check_service_running()
        results["service_running_detail"] = "服务正在运行" if results["service_running"] else "服务未运行"
        
        # 2. 检查服务端口是否监听
        results["port_listening"] = self.check_port_listening(host, port)
        results["port_listening_detail"] = f"端口 {host}:{port} 正在监听" if results["port_listening"] else f"端口 {host}:{port} 未监听"
        
        # 3. 检查控制台端口是否监听
        results["console_port_listening"] = self.check_port_listening(host, console_port)
        results["console_port_listening_detail"] = f"控制台端口 {host}:{console_port} 正在监听" if results["console_port_listening"] else f"控制台端口 {host}:{console_port} 未监听"
        
        # 4. 调用健康检查API
        health_status, health_response = self.check_health_api(host, port, secure, credentials)
        results["health_api"] = health_status
        results["health_api_detail"] = f"健康检查API调用成功，响应：{health_response}" if health_status else f"健康检查API调用失败，响应：{health_response}"
        
        # 5. 检查mc命令是否可用
        results["mc_available"] = self.check_mc_command()
        results["mc_available_detail"] = "mc命令可用" if results["mc_available"] else "mc命令不可用"
        
        # 6. 测试存储桶访问（如果mc可用且健康检查API通过）
        if results["mc_available"] and results["health_api"]:
            bucket_status, bucket_message = self.check_bucket_access(host, port, secure, credentials)
            results["bucket_access"] = bucket_status
            results["bucket_access_detail"] = bucket_message
        else:
            results["bucket_access"] = False
            results["bucket_access_detail"] = "mc命令不可用或健康检查API未通过，跳过存储桶访问测试"
        
        # 7. 计算总体状态（至少服务运行、端口监听和健康检查API通过）
        results["overall_status"] = (
            results["service_running"] and 
            results["port_listening"] and 
            results["health_api"]
        )
        
        self.logger.info(f"综合健康检查完成，总体状态：{'正常' if results['overall_status'] else '异常'}")
        
        # 打印详细检查结果
        self.logger.info("详细健康检查结果：")
        for key, value in results.items():
            if "detail" not in key:
                self.logger.info(f"  {key}: {'✓' if value else '✗'} {results.get(f'{key}_detail', '')}")
        
        return results
