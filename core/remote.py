import paramiko
import os
import subprocess
import socket
import traceback
from core.logger import Logger
from concurrent.futures import ThreadPoolExecutor

class RemoteExecutor:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
        self.ssh_clients = {}
    
    def check_ssh_connection(self, host, port=22, username='root', key_file=None, password=None, timeout=5):
        """
        检查SSH连接是否可用
        
        Args:
            host: 远程主机IP或主机名
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码，默认为None
            timeout: 超时时间，默认为5秒
        
        Returns:
            bool: True表示连接成功，False表示连接失败
        """
        self.logger.info(f"检查SSH连接：{username}@{host}:{port}")
        
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.logger.debug(f"创建SSH客户端实例，设置自动添加主机密钥策略")
            
            if password:
                self.logger.debug(f"使用密码连接")
                self.logger.debug(f"尝试使用密码连接：{username}@{host}:{port}")
                client.connect(host, port, username, password, timeout=timeout, look_for_keys=False)
            elif key_file:
                key_path = os.path.expanduser(key_file)
                # 如果提供的是公钥文件路径，自动转换为私钥文件路径
                if key_path.endswith('.pub'):
                    key_path = key_path[:-4]  # 移除.pub后缀
                    self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{key_file} -> {key_path}")
                self.logger.debug(f"使用SSH密钥文件：{key_path}")
                self.logger.debug(f"尝试使用密钥文件连接：{username}@{host}:{port}")
                client.connect(host, port, username, key_filename=key_path, timeout=timeout, look_for_keys=False)
            else:
                # 尝试使用默认密钥文件
                self.logger.debug(f"使用默认密钥文件连接")
                self.logger.debug(f"尝试使用默认密钥文件连接：{username}@{host}:{port}")
                client.connect(host, port, username, timeout=timeout)
            
            self.logger.debug(f"SSH连接成功建立，正在关闭连接")
            client.close()
            self.logger.info(f"SSH连接成功：{username}@{host}:{port}")
            return True
        except paramiko.ssh_exception.AuthenticationException as e:
            self.logger.debug(f"认证失败详情：{e}")
            self.logger.error(f"SSH连接失败：{username}@{host}:{port}，认证错误：{e}")
            return False
        except paramiko.ssh_exception.SSHException as e:
            self.logger.debug(f"SSH协议错误详情：{e}")
            self.logger.error(f"SSH连接失败：{username}@{host}:{port}，SSH错误：{e}")
            return False
        except socket.timeout as e:
            self.logger.debug(f"连接超时详情：{e}")
            self.logger.error(f"SSH连接失败：{username}@{host}:{port}，连接超时：{e}")
            return False
        except Exception as e:
            self.logger.debug(f"其他错误详情：{traceback.format_exc()}")
            self.logger.error(f"SSH连接失败：{username}@{host}:{port}，错误：{e}")
            return False
    
    def check_ssh_trust(self, host, port=22, username='root', key_file=None, password=None):
        """
        检查SSH互信是否已建立
        
        Args:
            host: 远程主机IP或主机名
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码
        
        Returns:
            bool: True表示互信已建立，False表示未建立
        """
        self.logger.info(f"检查SSH互信：{username}@{host}:{port}")
        
        try:
            # 1. 首先尝试使用SSH密钥连接（检查是否已建立互信）
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.logger.debug(f"创建SSH客户端实例，设置了自动添加主机密钥策略")
            
            if key_file or password:
                # 获取公钥内容（无论是否有私钥文件）
                public_key_content = None
                if key_file:
                    # 如果提供的是公钥文件路径，直接使用；否则尝试获取对应的公钥文件
                    key_path = os.path.expanduser(key_file)
                    if key_path.endswith('.pub'):
                        # 直接读取公钥文件
                        if os.path.exists(key_path):
                            with open(key_path, 'r') as f:
                                public_key_content = f.read().strip()
                            self.logger.debug(f"从公钥文件读取公钥内容")
                        else:
                            self.logger.debug(f"指定的公钥文件不存在：{key_path}")
                    else:
                        # 尝试读取对应的公钥文件
                        pub_key_path = key_path + '.pub'
                        if os.path.exists(pub_key_path):
                            with open(pub_key_path, 'r') as f:
                                public_key_content = f.read().strip()
                            self.logger.debug(f"从对应的公钥文件读取公钥内容：{pub_key_path}")
                        else:
                            self.logger.debug(f"指定的私钥文件对应的公钥文件不存在：{pub_key_path}")
                
                # 如果有私钥文件，尝试使用私钥连接
                if key_file:
                    # 如果提供的是公钥文件路径，自动转换为私钥文件路径
                    private_key_file = os.path.expanduser(key_file)
                    if private_key_file.endswith('.pub'):
                        private_key_file = private_key_file[:-4]  # 移除.pub后缀
                        self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{key_file} -> {private_key_file}")
                    
                    # 检查私钥文件是否存在
                    if os.path.exists(private_key_file):
                        self.logger.debug(f"尝试使用指定的SSH私钥文件连接：{private_key_file}")
                        try:
                            client.connect(host, port, username, key_filename=private_key_file, timeout=5, look_for_keys=False, allow_agent=False)
                            self.logger.debug(f"使用SSH私钥连接成功，SSH互信已建立")
                            client.close()
                            self.logger.info(f"SSH互信已建立：{username}@{host}:{port}")
                            return True
                        except paramiko.ssh_exception.AuthenticationException as e:
                            self.logger.debug(f"使用指定私钥认证失败：{e}")
                        except Exception as e:
                            self.logger.debug(f"使用指定私钥连接失败：{e}")
                
                # 尝试使用默认密钥文件连接
                self.logger.debug(f"尝试使用默认密钥文件连接")
                try:
                    # 先检查默认密钥文件是否存在
                    default_key_files = [
                        os.path.expanduser("~/.ssh/id_rsa"),
                        os.path.expanduser("~/.ssh/id_dsa"),
                        os.path.expanduser("~/.ssh/id_ecdsa"),
                        os.path.expanduser("~/.ssh/id_ed25519")
                    ]
                    has_default_key = any(os.path.exists(key_file) for key_file in default_key_files)
                    
                    if has_default_key:
                        client.connect(host, port, username, timeout=5, look_for_keys=True, allow_agent=True, password=None)
                        self.logger.debug(f"使用默认密钥连接成功，SSH互信已建立")
                        client.close()
                        self.logger.info(f"SSH互信已建立：{username}@{host}:{port}")
                        return True
                    else:
                        self.logger.debug(f"没有找到默认的SSH密钥文件")
                except paramiko.ssh_exception.AuthenticationException as e:
                    self.logger.debug(f"使用默认密钥认证失败：{e}")
                except Exception as e:
                    self.logger.debug(f"使用默认密钥连接失败：{e}")
            
            # 2. 如果有公钥内容和密码，检查公钥是否已经在服务器上
            if public_key_content and password:
                self.logger.debug(f"有公钥内容，使用密码连接服务器检查公钥是否已存在")
                try:
                    # 重新创建客户端实例
                    client = paramiko.SSHClient()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    client.connect(host, port, username, password, timeout=5, look_for_keys=False, allow_agent=False)
                    
                    # 检查服务器上的authorized_keys文件是否包含公钥
                    command = "cat ~/.ssh/authorized_keys 2>/dev/null || echo ''"
                    stdin, stdout, stderr = client.exec_command(command)
                    server_authorized_keys = stdout.read().decode('utf-8')
                    
                    # 检查公钥是否已存在（只比较公钥部分，忽略注释）
                    pub_key_part = public_key_content.split()[1] if ' ' in public_key_content else public_key_content
                    if pub_key_part in server_authorized_keys:
                        self.logger.debug(f"公钥已存在于服务器的authorized_keys文件中")
                        client.close()
                        self.logger.info(f"SSH互信已建立：{username}@{host}:{port}")
                        return True
                    else:
                        self.logger.debug(f"公钥不存在于服务器的authorized_keys文件中")
                    
                    client.close()
                except Exception as e:
                    self.logger.debug(f"检查服务器公钥时出错：{e}")
            
            # 3. 如果密钥连接失败，且提供了密码但没有公钥内容，则尝试使用密码连接（仅用于检查连接，不代表互信）
            if password and not public_key_content:
                self.logger.debug(f"密钥连接失败，且无法检查公钥是否存在，尝试使用密码连接（仅检查连接可用性）")
                try:
                    client = paramiko.SSHClient()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    client.connect(host, port, username, password, timeout=5, look_for_keys=False, allow_agent=False)
                    self.logger.debug(f"使用密码连接成功，但无法确认是否已建立SSH互信")
                    client.close()
                    self.logger.info(f"SSH互信未确认：{username}@{host}:{port}")
                    return False
                except Exception as e:
                    self.logger.debug(f"使用密码连接也失败：{e}")
            
            # 4. 如果所有连接尝试都失败或无法确认互信
            self.logger.info(f"SSH互信未建立：{username}@{host}:{port}")
            return False
            
        except paramiko.ssh_exception.AuthenticationException as e:
            self.logger.debug(f"认证失败详情：{e}")
            self.logger.info(f"SSH互信未建立：{username}@{host}:{port}")
            return False
        except paramiko.ssh_exception.SSHException as e:
            self.logger.debug(f"SSH协议错误：{e}")
            self.logger.error(f"检查SSH互信失败：{username}@{host}:{port}，SSH错误：{e}")
            return False
        except socket.timeout as e:
            self.logger.debug(f"连接超时：{e}")
            self.logger.error(f"检查SSH互信失败：{username}@{host}:{port}，连接超时：{e}")
            return False
        except Exception as e:
            import traceback
            self.logger.debug(f"其他错误详情：{traceback.format_exc()}")
            self.logger.error(f"检查SSH互信失败：{username}@{host}:{port}，错误：{e}")
            return False
    
    def setup_ssh_trust(self, host, port=22, username='root', key_file=None, password=None):
        """
        建立SSH互信
        
        Args:
            host: 远程主机IP或主机名
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码
        
        Returns:
            bool: True表示互信建立成功，False表示失败
        """
        self.logger.info(f"建立SSH互信：{username}@{host}:{port}")
        
        # 检查本地是否存在SSH密钥对
        if not key_file:
            key_file = os.path.expanduser("~/.ssh/id_rsa")
            self.logger.debug(f"未指定密钥文件，使用默认路径：{key_file}")
        else:
            key_file = os.path.expanduser(key_file)
            # 如果提供的是公钥文件路径，自动转换为私钥文件路径
            if key_file.endswith('.pub'):
                key_file = key_file[:-4]  # 移除.pub后缀
                self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{key_file}")
            self.logger.debug(f"使用指定的密钥文件：{key_file}")
        
        if not os.path.exists(key_file):
            self.logger.info(f"本地SSH密钥对不存在，生成新的密钥对：{key_file}")
            # 生成SSH密钥对
            cmd = f"ssh-keygen -t rsa -b 2048 -f {key_file} -N ''"
            self.logger.debug(f"执行命令生成密钥对：{cmd}")
            try:
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True)
                self.logger.debug(f"密钥对生成命令输出：stdout={result.stdout.decode()}, stderr={result.stderr.decode()}")
                self.logger.info(f"SSH密钥对生成成功：{key_file}")
            except subprocess.CalledProcessError as e:
                self.logger.debug(f"密钥对生成命令失败，返回码：{e.returncode}, stdout={e.stdout.decode()}, stderr={e.stderr.decode()}")
                self.logger.error(f"生成SSH密钥对失败：{e}")
                return False
        else:
            self.logger.debug(f"SSH密钥对已存在：{key_file}")
        
        # 获取公钥内容
        public_key_file = f"{key_file}.pub"
        self.logger.debug(f"公钥文件路径：{public_key_file}")
        try:
            with open(public_key_file, 'r') as f:
                public_key = f.read().strip()
            self.logger.debug(f"读取到公钥内容：{public_key}")
        except Exception as e:
            self.logger.debug(f"读取公钥文件失败详情：{traceback.format_exc()}")
            self.logger.error(f"读取公钥文件失败：{e}")
            return False
        
        # 先检查公钥是否已经存在于服务器的authorized_keys文件中
        try:
            self.logger.debug(f"检查公钥是否已存在于服务器：{username}@{host}:{port}")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, port, username, password, timeout=5, look_for_keys=False, allow_agent=False)
            
            # 检查服务器上的authorized_keys文件是否包含公钥
            command = "cat ~/.ssh/authorized_keys 2>/dev/null || echo ''"
            stdin, stdout, stderr = client.exec_command(command)
            server_authorized_keys = stdout.read().decode('utf-8')
            
            # 检查公钥是否已存在（只比较公钥部分，忽略注释）
            pub_key_part = public_key.split()[1] if ' ' in public_key else public_key
            if pub_key_part in server_authorized_keys:
                self.logger.debug(f"公钥已存在于服务器的authorized_keys文件中，无需再次添加")
                client.close()
                self.logger.info(f"SSH互信已建立：{username}@{host}:{port}")
                return True
            
            client.close()
        except Exception as e:
            self.logger.debug(f"检查服务器公钥时出错：{e}")
            # 继续执行后续操作，尝试添加公钥
        
        # 将公钥添加到远程主机的authorized_keys文件
        try:
            self.logger.debug(f"开始连接远程主机：{username}@{host}:{port}")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.logger.debug(f"创建SSH客户端实例，设置自动添加主机密钥策略")
            
            if password:
                self.logger.debug(f"使用密码连接到远程主机")
                client.connect(host, port, username, password, timeout=5)
                self.logger.debug(f"成功连接到远程主机")
            else:
                self.logger.error("建立SSH互信需要密码")
                return False
            
            # 创建.ssh目录
            self.logger.debug(f"在远程主机创建.ssh目录并设置权限")
            stdin, stdout, stderr = client.exec_command("mkdir -p ~/.ssh && chmod 700 ~/.ssh")
            exit_code = stdout.channel.recv_exit_status()
            stderr_content = stderr.read().decode('utf-8').strip()
            if exit_code != 0:
                self.logger.debug(f"创建.ssh目录失败，返回码：{exit_code}, 错误：{stderr_content}")
                client.close()
                return False
            self.logger.debug(f".ssh目录创建成功")
            
            # 将公钥添加到authorized_keys
            command = f"echo '{public_key}' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
            self.logger.debug(f"执行命令添加公钥：{command}")
            stdin, stdout, stderr = client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            stderr_content = stderr.read().decode('utf-8').strip()
            
            if exit_code != 0:
                self.logger.debug(f"添加公钥失败，返回码：{exit_code}, 错误：{stderr_content}")
                self.logger.error(f"添加公钥失败，错误：{stderr_content}")
                client.close()
                return False
            
            self.logger.debug(f"公钥添加成功，正在关闭连接")
            client.close()
            self.logger.info(f"SSH互信建立成功：{username}@{host}:{port}")
            return True
            
        except paramiko.ssh_exception.AuthenticationException as e:
            self.logger.debug(f"认证失败详情：{e}")
            self.logger.error(f"建立SSH互信失败，认证错误：{e}")
            return False
        except paramiko.ssh_exception.SSHException as e:
            self.logger.debug(f"SSH协议错误详情：{e}")
            self.logger.error(f"建立SSH互信失败，SSH错误：{e}")
            return False
        except socket.timeout as e:
            self.logger.debug(f"连接超时详情：{e}")
            self.logger.error(f"建立SSH互信失败，连接超时：{e}")
            return False
        except Exception as e:
            self.logger.debug(f"其他错误详情：{traceback.format_exc()}")
            self.logger.error(f"建立SSH互信失败：{e}")
            return False
    
    def execute_command(self, host, command, port=22, username='root', key_file=None, password=None, timeout=30):
        """
        执行远程命令
        
        Args:
            host: 远程主机IP或主机名
            command: 要执行的命令
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码
            timeout: 超时时间，默认为30秒
        
        Returns:
            tuple: (exit_code, stdout, stderr)
        """
        self.logger.debug(f"执行远程命令：{username}@{host}:{port}，命令：{command}")
        
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 处理密钥文件路径（如果提供的是公钥文件，自动转换为私钥文件）
            private_key_file = None
            if key_file:
                private_key_file = os.path.expanduser(key_file)
                if private_key_file.endswith('.pub'):
                    temp_private = private_key_file[:-4]  # 移除.pub后缀
                    self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{private_key_file} -> {temp_private}")
                    private_key_file = temp_private
            
            # 优先尝试使用SSH密钥连接（如果已建立互信）
            if (private_key_file or password) and not password:
                if private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            # 同时提供了密码和密钥，先尝试密钥连接
            elif private_key_file and password:
                try:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5, look_for_keys=False, allow_agent=False)
                    self.logger.debug(f"优先使用SSH密钥连接成功")
                except paramiko.ssh_exception.AuthenticationException:
                    self.logger.debug(f"SSH密钥连接失败，尝试使用密码连接")
                    client.connect(host, port, username, password, timeout=5)
            # 只提供了密码或都没提供
            else:
                if password:
                    client.connect(host, port, username, password, timeout=5)
                elif private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            
            stdout_str = stdout.read().decode('utf-8').strip()
            stderr_str = stderr.read().decode('utf-8').strip()
            
            client.close()
            
            if exit_code == 0:
                self.logger.debug(f"命令执行成功：{command}，输出：{stdout_str}")
            else:
                self.logger.warning(f"命令执行失败：{command}，退出码：{exit_code}，错误：{stderr_str}")
            
            return (exit_code, stdout_str, stderr_str)
        
        except Exception as e:
            self.logger.error(f"执行远程命令失败：{username}@{host}:{port}，命令：{command}，错误：{e}")
            return (1, "", str(e))
    
    def upload_file(self, local_path, remote_path, host, port=22, username='root', key_file=None, password=None):
        """
        上传文件到远程主机
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件路径
            host: 远程主机IP或主机名
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码
        
        Returns:
            bool: True表示上传成功，False表示失败
        """
        self.logger.info(f"上传文件：{local_path} -> {username}@{host}:{port}:{remote_path}")
        
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 处理密钥文件路径（如果提供的是公钥文件，自动转换为私钥文件）
            private_key_file = None
            if key_file:
                private_key_file = os.path.expanduser(key_file)
                if private_key_file.endswith('.pub'):
                    temp_private = private_key_file[:-4]  # 移除.pub后缀
                    self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{private_key_file} -> {temp_private}")
                    private_key_file = temp_private
            
            # 优先尝试使用SSH密钥连接（如果已建立互信）
            if (private_key_file or password) and not password:
                if private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            # 同时提供了密码和密钥，先尝试密钥连接
            elif private_key_file and password:
                try:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5, look_for_keys=False, allow_agent=False)
                    self.logger.debug(f"优先使用SSH密钥连接成功")
                except paramiko.ssh_exception.AuthenticationException:
                    self.logger.debug(f"SSH密钥连接失败，尝试使用密码连接")
                    client.connect(host, port, username, password, timeout=5)
            # 只提供了密码或都没提供
            else:
                if password:
                    client.connect(host, port, username, password, timeout=5)
                elif private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            
            sftp = client.open_sftp()
            sftp.put(local_path, remote_path)
            sftp.close()
            client.close()
            
            self.logger.info(f"文件上传成功：{local_path} -> {username}@{host}:{port}:{remote_path}")
            return True
        
        except Exception as e:
            self.logger.error(f"文件上传失败：{local_path} -> {username}@{host}:{port}:{remote_path}，错误：{e}")
            return False
    
    def download_file(self, remote_path, local_path, host, port=22, username='root', key_file=None, password=None):
        """
        从远程主机下载文件
        
        Args:
            remote_path: 远程文件路径
            local_path: 本地文件路径
            host: 远程主机IP或主机名
            port: SSH端口，默认为22
            username: 用户名，默认为root
            key_file: SSH私钥文件路径
            password: 密码
        
        Returns:
            bool: True表示下载成功，False表示失败
        """
        self.logger.info(f"下载文件：{username}@{host}:{port}:{remote_path} -> {local_path}")
        
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # 处理密钥文件路径（如果提供的是公钥文件，自动转换为私钥文件）
            private_key_file = None
            if key_file:
                private_key_file = os.path.expanduser(key_file)
                if private_key_file.endswith('.pub'):
                    temp_private = private_key_file[:-4]  # 移除.pub后缀
                    self.logger.debug(f"将公钥文件路径转换为私钥文件路径：{private_key_file} -> {temp_private}")
                    private_key_file = temp_private
            
            # 优先尝试使用SSH密钥连接（如果已建立互信）
            if (private_key_file or password) and not password:
                if private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            # 同时提供了密码和密钥，先尝试密钥连接
            elif private_key_file and password:
                try:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5, look_for_keys=False, allow_agent=False)
                    self.logger.debug(f"优先使用SSH密钥连接成功")
                except paramiko.ssh_exception.AuthenticationException:
                    self.logger.debug(f"SSH密钥连接失败，尝试使用密码连接")
                    client.connect(host, port, username, password, timeout=5)
            # 只提供了密码或都没提供
            else:
                if password:
                    client.connect(host, port, username, password, timeout=5)
                elif private_key_file:
                    client.connect(host, port, username, key_filename=private_key_file, timeout=5)
                else:
                    client.connect(host, port, username, timeout=5)
            
            sftp = client.open_sftp()
            sftp.get(remote_path, local_path)
            sftp.close()
            client.close()
            
            self.logger.info(f"文件下载成功：{username}@{host}:{port}:{remote_path} -> {local_path}")
            return True
        
        except Exception as e:
            self.logger.error(f"文件下载失败：{username}@{host}:{port}:{remote_path} -> {local_path}，错误：{e}")
            return False
    
    def execute_parallel(self, tasks, max_workers=5):
        """
        并行执行远程命令
        
        Args:
            tasks: 任务列表，每个任务是一个字典，包含host, command, port, username, key_file, password等参数
            max_workers: 最大工作线程数，默认为5
        
        Returns:
            list: 任务执行结果列表，每个结果是一个字典，包含task和result
        """
        self.logger.info(f"并行执行 {len(tasks)} 个远程命令，最大工作线程数：{max_workers}")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_task = {}
            for task in tasks:
                future = executor.submit(
                    self.execute_command,
                    host=task['host'],
                    command=task['command'],
                    port=task.get('port', 22),
                    username=task.get('username', 'root'),
                    key_file=task.get('key_file'),
                    password=task.get('password'),
                    timeout=task.get('timeout', 30)
                )
                future_to_task[future] = task
            
            # 获取结果
            for future in future_to_task:
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append({
                        'task': task,
                        'result': result
                    })
                except Exception as e:
                    results.append({
                        'task': task,
                        'result': (1, "", str(e))
                    })
        
        self.logger.info(f"并行执行完成，共 {len(results)} 个结果")
        return results
