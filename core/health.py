import subprocess
import requests
import socket
from core.logger import Logger

class HealthChecker:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
    
    def check_port_listening(self, host, port, timeout=5, retry_count=5, retry_delay=5):
        """
        检查指定端口是否监听（带重试机制）
        
        Args:
            host: 主机IP或域名
            port: 端口号
            timeout: 超时时间，默认为5秒
            retry_count: 重试次数，默认为3次
            retry_delay: 重试间隔时间，默认为2秒
        
        Returns:
            bool: True表示端口正在监听，False表示未监听
        """
        import time
        
        for attempt in range(retry_count):
            self.logger.info(f"检查端口 {host}:{port} 是否监听（第 {attempt + 1}/{retry_count} 次）...")
            
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(timeout)
                    result = s.connect_ex((host, port))
                    if result == 0:
                        self.logger.info(f"端口 {host}:{port} 正在监听")
                        return True
                    else:
                        self.logger.warning(f"端口 {host}:{port} 未监听（第 {attempt + 1}/{retry_count} 次）")
            except Exception as e:
                self.logger.error(f"检查端口 {host}:{port} 失败（第 {attempt + 1}/{retry_count} 次）：{e}")
            
            # 如果不是最后一次尝试，等待一段时间后重试
            if attempt < retry_count - 1:
                self.logger.info(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
        
        self.logger.warning(f"端口 {host}:{port} 未监听（已重试 {retry_count} 次）")
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
    
    def check_health_api(self, host, port=9000, secure=False, credentials=None, timeout=5, retry_count=5, retry_delay=5):
        """
        调用MinIO健康检查API（带重试机制）
        
        Args:
            host: 主机IP或域名
            port: 端口号，默认为9000
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息（可选，健康检查API通常不需要认证）
            timeout: 超时时间，默认为5秒
            retry_count: 重试次数，默认为5次
            retry_delay: 重试间隔时间，默认为5秒
        
        Returns:
            tuple: (status, response)，status为True表示健康检查通过，response为响应内容
        """
        import time
        
        protocol = "https" if secure else "http"
        url = f"{protocol}://{host}:{port}/minio/health/live"
        
        for attempt in range(retry_count):
            self.logger.info(f"调用健康检查API：{url}（第 {attempt + 1}/{retry_count} 次）")
            
            try:
                # 健康检查API通常不需要认证，移除认证参数
                response = requests.get(url, timeout=timeout, verify=False)  # verify=False用于自签名证书
                
                if response.status_code == 200:
                    self.logger.info(f"健康检查API调用成功，状态码：{response.status_code}")
                    return (True, response.text)
                else:
                    self.logger.warning(f"健康检查API调用失败（第 {attempt + 1}/{retry_count} 次），状态码：{response.status_code}，响应：{response.text}")
                    
            except Exception as e:
                self.logger.error(f"健康检查API调用失败（第 {attempt + 1}/{retry_count} 次）：{e}")
            
            # 如果不是最后一次尝试，等待一段时间后重试
            if attempt < retry_count - 1:
                self.logger.info(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
        
        self.logger.error(f"健康检查API调用失败（已重试 {retry_count} 次）")
        return (False, "健康检查API调用失败")
    
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
    
    def create_buckets(self, host, port=9000, secure=False, credentials=None, buckets=None):
        """
        根据配置创建存储桶并设置策略和配额
        
        Args:
            host: 主机IP或域名
            port: 端口号，默认为9000
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息，包含root_user和root_password
            buckets: 存储桶配置列表
        
        Returns:
            tuple: (status, message)，status为True表示所有存储桶创建成功，message为详细信息
        """
        self.logger.info("开始创建存储桶")
        
        # 检查mc命令是否可用
        if not self.check_mc_command():
            return (False, "mc命令不可用，无法创建存储桶")
        
        if not buckets:
            self.logger.info("没有需要创建的存储桶配置")
            return (True, "没有需要创建的存储桶配置")
        
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
            
            for bucket in buckets:
                bucket_name = bucket.get("name")
                policy = bucket.get("policy", "private")
                quota = bucket.get("quota", 0)
                
                if not bucket_name:
                    self.logger.warning("跳过无效的存储桶配置：缺少name字段")
                    continue
                
                self.logger.info(f"处理存储桶：{bucket_name}")
                
                # 创建存储桶
                self.logger.info(f"创建存储桶：{bucket_name}")
                cmd = ["mc", "mb", f"{alias_name}/{bucket_name}"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    self.logger.info(f"存储桶 {bucket_name} 创建成功")
                else:
                    self.logger.warning(f"存储桶 {bucket_name} 可能已存在或创建失败：{result.stderr.strip()}")
                
                # 设置存储桶策略
                self.logger.info(f"设置存储桶 {bucket_name} 的策略为：{policy}")
                if policy == "public":
                    cmd = ["mc", "policy", "set", "public", f"{alias_name}/{bucket_name}"]
                else:
                    cmd = ["mc", "policy", "set", "private", f"{alias_name}/{bucket_name}"]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                self.logger.info(f"存储桶 {bucket_name} 策略设置成功")
                
                # 设置存储桶配额
                if quota > 0:
                    self.logger.info(f"设置存储桶 {bucket_name} 的配额为：{quota}GB")
                    try:
                        # 使用友好的单位格式（如"100G"）和正确的--size参数
                        cmd = ["mc", "quota", "set", f"{alias_name}/{bucket_name}", "--size", f"{quota}G"]
                        result = subprocess.run(cmd, capture_output=True, text=True, check=False)  # 不使用check=True，自己处理返回码
                        
                        cmd_str = " ".join(cmd)
                        stdout = result.stdout.strip() if result.stdout else "(空)"
                        stderr = result.stderr.strip() if result.stderr else "(空)"
                        
                        # 检查返回码和输出信息
                        if result.returncode == 0:
                            self.logger.info(f"存储桶 {bucket_name} 配额设置成功")
                        else:
                            # 特殊处理：如果输出包含"mc: Please use 'mc anonymous'"，可能是误报的警告
                            if "mc: Please use 'mc anonymous'" in stdout:
                                self.logger.warning(f"存储桶 {bucket_name} 配额设置可能已成功，但收到警告信息")
                                self.logger.warning(f"执行的命令：{cmd_str}")
                                self.logger.warning(f"命令输出：{stdout}")
                                # 继续执行，不中断流程
                            else:
                                # 其他错误情况
                                self.logger.warning(f"存储桶 {bucket_name} 配额设置失败：返回码：{result.returncode}")
                                self.logger.warning(f"执行的命令：{cmd_str}")
                                self.logger.warning(f"命令输出：{stdout}")
                                self.logger.warning(f"错误信息：{stderr}")
                                self.logger.warning("可能的原因：磁盘空间不足、单位格式不正确或权限问题")
                                # 不中断流程，继续执行
                    except Exception as e:
                        # 捕获其他可能的异常
                        cmd_str = " ".join(cmd)
                        self.logger.warning(f"存储桶 {bucket_name} 配额设置时发生异常：{str(e)}")
                        self.logger.warning(f"执行的命令：{cmd_str}")
                        # 不中断流程，继续执行
                else:
                    self.logger.info(f"存储桶 {bucket_name} 不设置配额（配额为0或未指定）")
            
            return (True, "所有存储桶创建和配置完成")
            
        except Exception as e:
            self.logger.error(f"存储桶创建失败：{e}")
            return (False, f"存储桶创建失败：{e}")
    
    def run_comprehensive_check(self, host, port=9000, console_port=9001, secure=False, credentials=None, buckets=None):
        """
        运行综合健康检查
        
        Args:
            host: 主机IP或域名
            port: 服务端口，默认为9000
            console_port: 控制台端口，默认为9001
            secure: 是否使用HTTPS，默认为False
            credentials: 认证信息，包含root_user和root_password
            buckets: 存储桶配置列表（可选）
        
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
        
        # 8. 如果健康检查通过且有存储桶配置，则创建实际存储桶
        if results["overall_status"] and buckets:
            self.logger.info("\n开始创建配置的实际存储桶...")
            bucket_create_status, bucket_create_message = self.create_buckets(host, port, secure, credentials, buckets)
            if bucket_create_status:
                self.logger.info(f"存储桶创建结果：{bucket_create_message}")
            else:
                self.logger.warning(f"存储桶创建过程中出现问题：{bucket_create_message}")
        
        return results
