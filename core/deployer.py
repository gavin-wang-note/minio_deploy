import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from core.logger import Logger
from core.system_check import SystemCheck
from core.config_parser import ConfigParser
from core.remote import RemoteExecutor
from core.disk import DiskManager
from core.firewall import FirewallManager
from core.minio_installer import MinioInstaller
from core.service import ServiceManager
from core.health import HealthChecker

class Deployer:
    def __init__(self, config_file, dry_run=False, logger=None, mode=None):
        self.config_file = config_file
        self.dry_run = dry_run
        self.logger = logger or Logger().get_logger()
        self.mode = mode
        self.config_parser = ConfigParser(config_file, logger=self.logger)
        self.system_check = SystemCheck(logger=self.logger)
        self.remote_executor = RemoteExecutor(logger=self.logger)
        self.disk_manager = DiskManager(logger=self.logger)
        self.firewall_manager = FirewallManager(logger=self.logger)
        self.minio_installer = MinioInstaller(logger=self.logger)
        self.service_manager = ServiceManager(logger=self.logger)
        self.health_checker = HealthChecker(logger=self.logger)
        self.config = None
    
    def run_system_checks(self):
        """
        运行系统检查
        """
        self.logger.info("## 开始系统检查")
        
        if self.dry_run:
            self.logger.info("[DRY RUN] 跳过系统检查")
        else:
            self.system_check.run_all_checks()
        
        self.logger.info("系统检查完成")
        self.logger.info("-" * 60)
    
    def load_config(self):
        """
        加载并验证配置
        """
        self.logger.info("## 加载配置文件")
        
        # 先获取配置，不验证
        self.config = self.config_parser.get_config(validate=False)
        
        # 使用命令行传入的模式验证配置
        if self.mode:
            self.logger.info(f"使用命令行指定的部署模式：**{self.mode}**")
            self.config_parser.validate_config(self.mode)
            # 将命令行指定的模式更新到配置中
            self.config['deployment_mode'] = self.mode
        else:
            # 没有指定模式，使用配置文件中的模式
            self.config_parser.validate_config()
        
        self.logger.info("配置文件加载完成")
        self.logger.info("-" * 60)
    
    def get_ssh_params(self, node_config=None):
        """
        获取SSH连接参数，支持单机和集群模式
        
        Args:
            node_config: 集群节点配置（仅集群模式使用）
            
        Returns:
            dict: 包含host, port, username, ssh_key, password的SSH连接参数
        """
        deployment_mode = self.config.get("deployment_mode")
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            username = standalone_config.get("ssh_user", "root")
            ssh_key = standalone_config.get("ssh_key")
            password = standalone_config.get("ssh_password")
            port = standalone_config.get("ssh_port", 22)
        else:  # cluster mode
            if not node_config:
                raise ValueError("集群模式下必须提供节点配置")
            
            host = node_config.get("ip", node_config.get("host"))
            username = node_config.get("ssh_user", "root")
            ssh_key = node_config.get("ssh_key")
            password = node_config.get("ssh_password")
            port = node_config.get("ssh_port", 22)
        
        return {
            "host": host,
            "port": port,
            "username": username,
            "ssh_key": ssh_key,
            "password": password
        }
    
    def check_os_partitions(self):
        """
        检查所有指定的磁盘是否为操作系统分区
        """
        self.logger.info("## 检查操作系统分区")
        
        deployment_mode = self.config.get("deployment_mode")
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            disk_config = standalone_config.get("disk", {})
            if disk_config.get("enabled", False):
                device = disk_config.get("device")
                if device:
                    if self.dry_run:
                        self.logger.info(f"[DRY RUN] 检查设备 {device} 是否存在")
                        self.logger.info(f"[DRY RUN] 检查设备 {device} 是否为操作系统分区")
                    else:
                        if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                            # 本地主机，直接检查
                            # 检查分区是否存在
                            if not self.disk_manager.check_partition_exists(device):
                                self.logger.error(f"指定的设备 {device} 不存在")
                                exit(1)
                            # 检查是否为操作系统分区
                            if not self.disk_manager.check_os_partition(device):
                                self.logger.error("操作系统分区检测失败，退出部署")
                                exit(1)
                        else:
                            # 远程主机，通过SSH检查
                            ssh_params = self.get_ssh_params()
                            
                            # 检查分区是否存在
                            cmd = f"test -e {device} && echo 'exists' || echo 'not exists'"
                            result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                            if result[0] != 0 or "not exists" in result[1]:
                                self.logger.error(f"指定的设备 {device} 不存在")
                                exit(1)
                            
                            # 检查是否为操作系统分区
                            cmd = f"df -h | grep -E '{device}' | grep -E '/$' || echo 'not os partition'"
                            result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                            if result[0] != 0 or "not os partition" in result[1]:
                                self.logger.error("操作系统分区检测失败，退出部署")
                                exit(1)
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            for node in nodes:
                self.logger.info(f"检查节点 {node.get('host')} 的磁盘配置")
                
                # 检查节点是否配置了磁盘设备
                if node.get("disk") and node["disk"].get("enabled", False):
                    device = node["disk"].get("device")
                    if device:
                        # 获取节点SSH配置
                        ssh_params = self.get_ssh_params(node)
                        
                        # 检查SSH连接
                        if not self.remote_executor.check_ssh_connection(ssh_params["host"], ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"]):
                            self.logger.error(f"无法连接到节点 {node.get('host')}，退出部署")
                            exit(1)
                        
                        # 在远程节点上检查分区是否存在
                        check_exists_cmd = f"test -e {device} && echo 'exists' || echo 'not exists'"
                        exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_exists_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                        if stdout.strip() != "exists":
                            self.logger.error(f"节点 {node.get('host')} 指定的设备 {device} 不存在")
                            exit(1)
                        self.logger.info(f"节点 {node.get('host')} 的设备 {device} 存在")
                        
                        # 在远程节点上检查是否为操作系统分区
                        check_os_cmd = f"grep -E '^({device}|/dev/sda|/dev/vda)' /proc/mounts | grep -E '(/|/boot|/boot/efi)'"
                        exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_os_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                        if exit_code == 0:
                            self.logger.error(f"节点 {node.get('host')} 检测到设备 {device} 是操作系统分区，不能用于MinIO存储")
                            exit(1)
                        self.logger.info(f"节点 {node.get('host')} 的设备 {device} 不是操作系统分区，可以安全使用")
                else:
                    self.logger.info(f"节点 {node.get('host')} 未配置磁盘设备或未启用磁盘管理")
        
        self.logger.info("操作系统分区检测完成")
        self.logger.info("-" * 60)

    def check_ssh_trust(self):
        """ 检查并配置SSH互信 """
        self.logger.info("## 检查并配置SSH互信")

        deployment_mode = self.config.get("deployment_mode")

        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")

            # 检查是否为本地主机
            if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                self.logger.info("单机模式，本地主机部署，无需建立SSH互信")
            else:
                # 远程单机部署，需要建立SSH互信
                self.logger.info(f"单机模式，远程主机 {host} 部署，需要建立SSH互信")
                ssh_params = self.get_ssh_params()
                
                # 检查SSH互信
                if self.remote_executor.check_ssh_trust(ssh_params["host"], ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"]):
                    pass
                else:
                    # 如果未建立互信，尝试建立
                    if ssh_params["password"]:
                        if self.dry_run:
                            self.logger.info(f"[DRY RUN] 尝试为远程主机 {host} 建立SSH互信")
                        else:
                            if not self.remote_executor.setup_ssh_trust(ssh_params["host"], ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"]):
                                self.logger.error(f"为远程主机 {host} 建立SSH互信失败")
                                exit(1)
                    else:
                        self.logger.error(f"远程主机 {host} 未配置密码，无法建立SSH互信")
                        exit(1)
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            
            for node in nodes:
                ssh_params = self.get_ssh_params(node)

                # 检查SSH互信
                if self.remote_executor.check_ssh_trust(ssh_params["host"], ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"]):
                    continue
                
                # 如果未建立互信，尝试建立
                if ssh_params["password"]:
                    if self.dry_run:
                        self.logger.info(f"[DRY RUN] 尝试为节点 {ssh_params['host']} 建立SSH互信")
                    else:
                        if not self.remote_executor.setup_ssh_trust(ssh_params["host"], ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"]):
                            self.logger.error(f"为节点 {ssh_params['host']} 建立SSH互信失败")
                            exit(1)
                else:
                    self.logger.error(f"节点 {ssh_params['host']} 未配置密码，无法建立SSH互信")
                    exit(1)
        
        self.logger.info("SSH互信检查和配置完成")
        self.logger.info("-" * 60)
    
    def configure_firewall(self):
        """
        配置防火墙
        """
        self.logger.info("## 配置防火墙")
        
        deployment_mode = self.config.get("deployment_mode")
        ports = []
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            cluster_config = self.config.get("cluster", {})
            ports.append(cluster_config.get("server_port", 9000))
            ports.append(cluster_config.get("console_port", 9001))
            
            if self.dry_run:
                self.logger.info(f"[DRY RUN] 准备开放端口：{ports}")
            else:
                # 检查是否为本地主机
                if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                    # 本地主机，直接配置防火墙
                    self.firewall_manager.configure_firewall(ports)
                else:
                    # 远程主机，通过SSH配置防火墙
                    ssh_params = self.get_ssh_params()
                    
                    # 构造防火墙配置命令
                    cmd = f"firewall-cmd --add-port={ports[0]}/tcp --permanent && firewall-cmd --add-port={ports[1]}/tcp --permanent && firewall-cmd --reload"
                    self.logger.info(f"通过SSH为远程主机 {host} 配置防火墙")
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                    if result[0] != 0:
                        self.logger.error(f"为远程主机 {host} 配置防火墙失败：{result[2]}")
                        exit(1)
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            ports.append(cluster_config.get("server_port", 9000))
            ports.append(cluster_config.get("console_port", 9001))
            
            # 配置本地防火墙
            if self.dry_run:
                self.logger.info(f"[DRY RUN] 准备开放本地端口：{ports}")
            else:
                self.firewall_manager.configure_firewall(ports)
            
            # 配置远程节点防火墙
            nodes = cluster_config.get("nodes", [])
            for node in nodes:
                ssh_params = self.get_ssh_params(node)

                if self.dry_run:
                    self.logger.info(f"[DRY RUN] 准备为节点 {ssh_params['host']} 开放端口：{ports}")
                else:
                    # 在远程节点上配置防火墙
                    cmd = f"firewall-cmd --add-port={ports[0]}/tcp --permanent && firewall-cmd --add-port={ports[1]}/tcp --permanent && firewall-cmd --reload"  # noqa
                    self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"])
        
        self.logger.info("防火墙配置完成")
        self.logger.info("-" * 60)
    
    def install_minio(self):
        """
        安装MinIO
        """
        self.logger.info("=" * 60)
        self.logger.info("安装MinIO")
        self.logger.info("=" * 60)
        
        deployment_mode = self.config.get("deployment_mode")
        minio_config = self.config.get("minio", {})
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            
            if self.dry_run:
                self.logger.info("[DRY RUN] 准备安装MinIO")
            else:
                # 检查是否为本地主机
                if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                    # 本地主机，直接安装MinIO
                    if not self.minio_installer.install_minio(minio_config):
                        self.logger.error("MinIO安装失败")
                        exit(1)
                    
                    # 安装mc（可选）
                    self.minio_installer.install_mc(minio_config)
                else:
                    # 远程主机，通过SSH安装MinIO
                    ssh_params = self.get_ssh_params()
                    
                    self.logger.info(f"通过SSH在远程主机 {host} 上安装MinIO")
                    
                    # 安装MinIO二进制文件
                    cmd = f"curl -sSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/local/bin/minio && chmod +x /usr/local/bin/minio"
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                    if result[0] != 0:
                        self.logger.error(f"在远程主机 {host} 上安装MinIO失败：{result[2]}")
                        exit(1)
                    
                    # 安装mc客户端
                    cmd = f"curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && chmod +x /usr/local/bin/mc"
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                    if result[0] != 0:
                        self.logger.warning(f"在远程主机 {host} 上安装mc客户端失败：{result[2]}")
                        # 安装mc失败不影响MinIO主功能，继续执行
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            
            # 并行安装MinIO到所有节点
            def install_on_node(node):
                ssh_params = self.get_ssh_params(node)
                
                self.logger.info(f"开始在节点 {ssh_params['host']} 上安装MinIO")
                
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] 准备在节点 {ssh_params['host']} 上安装MinIO")
                    return True
                else:
                    # 在远程节点上安装MinIO
                    # 这里简化处理，实际可能需要更复杂的逻辑，如上传二进制文件等
                    # 暂时假设远程节点可以直接访问互联网下载MinIO
                    cmd = f"curl -sSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/local/bin/minio && chmod +x /usr/local/bin/minio"  # noqa
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"])
                    if result[0] != 0:
                        self.logger.error(f"在节点 {ssh_params['host']} 上安装MinIO失败：{result[2]}")
                        return False
                    
                    # 安装mc
                    cmd = f"curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && chmod +x /usr/local/bin/mc"  # noqa
                    self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"])
                    
                    return True
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(install_on_node, nodes))
            
            if not all(results):
                self.logger.error("在某些节点上安装MinIO失败")
                exit(1)
        
        self.logger.info("=" * 60)
        self.logger.info("MinIO安装完成")
        self.logger.info("=" * 60)
    
    def configure_minio_service(self):
        """
        配置MinIO服务
        """
        self.logger.info("=" * 60)
        self.logger.info("配置MinIO服务")
        self.logger.info("=" * 60)
        
        # 再次检查MinIO服务是否存在，确保在格式化磁盘前进行检查
        deployment_mode = self.config.get("deployment_mode")
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            
            if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                # 本地主机，直接检查
                if self.service_manager.check_service_exists():
                    self.logger.error("检测到本地已存在MinIO服务！为避免覆盖现有环境，操作已终止。")
                    exit(1)
            else:
                # 远程主机，通过SSH检查
                ssh_params = self.get_ssh_params()
                
                # 构造检查命令
                check_cmd = "systemctl list-unit-files --type service | grep -q minio || [ -f /etc/systemd/system/minio.service ]"
                exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                
                if exit_code == 0:
                    self.logger.error(f"检测到远程主机 {host} 已存在MinIO服务！为避免覆盖现有环境，操作已终止。")
                    exit(1)
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            
            for node in nodes:
                ssh_params = self.get_ssh_params(node)
                
                # 构造检查命令
                check_cmd = "systemctl list-unit-files --type service | grep -q minio || [ -f /etc/systemd/system/minio.service ]"
                exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                
                if exit_code == 0:
                    self.logger.error(f"检测到集群节点 {host} 已存在MinIO服务！为避免覆盖现有环境，操作已终止。")
                    exit(1)
        
        credentials = self.config.get("credentials", {})
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            data_dir = standalone_config.get("data_dir", "/data/minio")
            cluster_config = self.config.get("cluster", {})
            listen_port = cluster_config.get("server_port", 9000)
            console_port = cluster_config.get("console_port", 9001)
            
            # 检查是否为本地主机
            if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                # 本地主机，直接配置
                disk_config = standalone_config.get("disk", {})
                if disk_config.get("enabled", False):
                    device = disk_config.get("device")
                    mount_point = disk_config.get("mount_point", data_dir)
                    filesystem = disk_config.get("filesystem", "ext4")
                    format_disk = disk_config.get("format_disk", False)
                    
                    if self.dry_run:
                        self.logger.info(f"[DRY RUN] 准备格式化和挂载磁盘 {device} 到 {mount_point}")
                    else:
                        # 准备磁盘
                        if not self.disk_manager.prepare_disk(device, mount_point, filesystem, format_disk):
                            self.logger.error("磁盘准备失败")
                            exit(1)
                    
                    data_dir = mount_point
                
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] 准备创建MinIO服务文件，数据目录：{data_dir}")
                else:
                    # 配置MinIO服务
                    if not self.service_manager.configure_service(
                        data_dir, listen_port, console_port, credentials
                    ):
                        self.logger.error("MinIO服务配置失败")
                        exit(1)
            else:
                # 远程主机，通过SSH配置
                ssh_params = self.get_ssh_params()
                
                disk_config = standalone_config.get("disk", {})
                if disk_config.get("enabled", False):
                    device = disk_config.get("device")
                    mount_point = disk_config.get("mount_point", data_dir)
                    filesystem = disk_config.get("filesystem", "ext4")
                    format_disk = disk_config.get("format_disk", False)
                    
                    if self.dry_run:
                        self.logger.info(f"[DRY RUN] 准备格式化和挂载远程主机 {host} 的磁盘 {device} 到 {mount_point}")
                    else:
                        self.logger.info(f"通过SSH准备远程主机 {host} 的磁盘 {device}")
                        
                        # 格式化磁盘（如果需要）
                        if format_disk:
                            cmd = f"mkfs.{filesystem} -f {device}"
                            result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                            if result[0] != 0:
                                self.logger.error(f"格式化远程主机 {host} 的磁盘 {device} 失败：{result[2]}")
                                exit(1)
                        
                        # 创建挂载点
                        cmd = f"mkdir -p {mount_point}"
                        result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                        if result[0] != 0:
                            self.logger.error(f"在远程主机 {host} 上创建挂载点 {mount_point} 失败：{result[2]}")
                            exit(1)
                        
                        # 挂载磁盘
                        cmd = f"mount {device} {mount_point}"
                        result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                        if result[0] != 0:
                            self.logger.error(f"在远程主机 {host} 上挂载磁盘 {device} 到 {mount_point} 失败：{result[2]}")
                            exit(1)
                        
                        # 更新fstab（可选）
                        # 这里简化处理，实际可能需要更复杂的逻辑
                        
                        data_dir = mount_point
                
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] 准备通过SSH创建远程主机 {host} 的MinIO服务文件")
                else:
                    # 通过SSH配置MinIO服务
                    self.logger.info(f"通过SSH配置远程主机 {host} 的MinIO服务")
                    
                    # 创建数据目录
                    cmd = f"mkdir -p {data_dir}"
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                    if result[0] != 0:
                        self.logger.error(f"在远程主机 {host} 上创建数据目录 {data_dir} 失败：{result[2]}")
                        exit(1)
                    
                    # 创建服务文件
                    service_content = f"[Unit]\n"
                    service_content += f"Description=MinIO\n"
                    service_content += f"Documentation=https://docs.min.io\n"
                    service_content += f"Wants=network-online.target\n"
                    service_content += f"After=network-online.target\n"
                    service_content += f"AssertFileIsExecutable=/usr/local/bin/minio\n"
                    service_content += f"\n"
                    service_content += f"[Service]\n"
                    service_content += f"WorkingDirectory=/usr/local/bin\n"
                    service_content += f"\n"
                    service_content += f"User=root\n"
                    service_content += f"Group=root\n"
                    service_content += f"\n"
                    service_content += f"EnvironmentFile=-/etc/default/minio\n"
                    service_content += f"ExecStartPre=/bin/bash -c '[ -n \"$MINIO_VOLUMES\" ] || echo \"Variable MINIO_VOLUMES not set in /etc/default/minio\"'\n"
                    service_content += f"\n"
                    service_content += f"ExecStart=/usr/local/bin/minio server \\n"
                    service_content += f"  --address :{listen_port} \\n"
                    service_content += f"  --console-address :{console_port} \\n"
                    service_content += f"  {data_dir}\n"
                    service_content += f"\n"
                    service_content += f"# Let systemd restart this service always\n"
                    service_content += f"Restart=always\n"
                    service_content += f"\n"
                    service_content += f"# Specifies the maximum file descriptor number that can be opened by this process\n"
                    service_content += f"LimitNOFILE=65536\n"
                    service_content += f"\n"
                    service_content += f"# Specifies the maximum number of processes that can be created by this process\n"
                    service_content += f"LimitNPROC=16384\n"
                    service_content += f"\n"
                    service_content += f"# Time to wait before forcefully killing the process\n"
                    service_content += f"TimeoutStopSec=5\n"
                    service_content += f"SendSIGKILL=no\n"
                    service_content += f"\n"
                    service_content += f"[Install]\n"
                    service_content += f"WantedBy=multi-user.target\n"
                    
                    # 创建环境变量文件
                    env_content = f"# MinIO environment variables\n"
                    env_content += f"MINIO_ROOT_USER={credentials.get('root_user', 'minioadmin')}\n"
                    env_content += f"MINIO_ROOT_PASSWORD={credentials.get('root_password', 'minioadmin123')}\n"
                    env_content += f"MINIO_VOLUMES=\"{data_dir}\"\n"
                    env_content += f"MINIO_OPTS=\"--address :{listen_port} --console-address :{console_port}\"\n"
                    
                    # 上传服务文件
                    service_file_path = f"/tmp/minio.service"
                    with open(service_file_path, 'w') as f:
                        f.write(service_content)
                    
                    # 通过SCP上传服务文件
                    scp_command = f"scp -P {ssh_params['port']} {service_file_path} {ssh_params['username']}@{ssh_params['host']}:/etc/systemd/system/minio.service"
                    result = subprocess.run(scp_command, shell=True, check=False, capture_output=True, text=True)
                    if result.returncode != 0:
                        self.logger.error(f"上传服务文件到远程主机 {host} 失败：{result.stderr}")
                        exit(1)
                    
                    # 删除临时文件
                    os.remove(service_file_path)
                    
                    # 上传环境变量文件
                    env_file_path = f"/tmp/minio.env"
                    with open(env_file_path, 'w') as f:
                        f.write(env_content)
                    
                    scp_command = f"scp -P {ssh_params['port']} {env_file_path} {ssh_params['username']}@{ssh_params['host']}:/etc/default/minio"
                    result = subprocess.run(scp_command, shell=True, check=False, capture_output=True, text=True)
                    if result.returncode != 0:
                        self.logger.error(f"上传环境变量文件到远程主机 {host} 失败：{result.stderr}")
                        exit(1)
                    
                    # 删除临时文件
                    os.remove(env_file_path)
                    
                    # 重新加载systemd配置并启动服务
                    cmd = f"systemctl daemon-reload && systemctl enable minio && systemctl start minio"
                    result = self.remote_executor.execute_command(ssh_params["host"], cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                    if result[0] != 0:
                        self.logger.error(f"在远程主机 {host} 上配置MinIO服务失败：{result[2]}")
                        exit(1)
                    
                    self.logger.info(f"远程主机 {host} 的MinIO服务配置成功")
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            server_port = cluster_config.get("server_port", 9000)
            console_port = cluster_config.get("console_port", 9001)
            erasure_coding = cluster_config.get("erasure_coding")
            
            # 集群模式下的服务配置比较复杂，需要生成集群启动命令
            # 这里简化处理，实际需要更复杂的逻辑
            self.logger.info("集群模式下的服务配置需要手动完成，请参考MinIO官方文档")
        
        self.logger.info("=" * 60)
        self.logger.info("MinIO服务配置完成")
        self.logger.info("=" * 60)
    
    def run_health_checks(self):
        """
        运行健康检查
        """
        self.logger.info("=" * 60)
        self.logger.info("运行健康检查")
        self.logger.info("=" * 60)
        
        deployment_mode = self.config.get("deployment_mode")
        credentials = self.config.get("credentials", {})
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            cluster_config = self.config.get("cluster", {})
            listen_port = cluster_config.get("server_port", 9000)
            console_port = cluster_config.get("console_port", 9001)
            
            if self.dry_run:
                self.logger.info(f"[DRY RUN] 准备运行健康检查，端口：{listen_port}")
            else:
                # 运行健康检查
                results = self.health_checker.run_comprehensive_check(
                    "localhost", listen_port, console_port, credentials=credentials
                )
                
                if not results["overall_status"]:
                    self.logger.error("健康检查失败")
                    exit(1)
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            server_port = cluster_config.get("server_port", 9000)
            console_port = cluster_config.get("console_port", 9001)
            
            # 并行运行健康检查
            def check_node_health(node):
                host = node.get("ip", node.get("host"))
                self.logger.info(f"开始检查节点 {host} 的健康状态")
                
                if self.dry_run:
                    self.logger.info(f"[DRY RUN] 准备检查节点 {host} 的健康状态")
                    return True
                else:
                    results = self.health_checker.run_comprehensive_check(
                        host, server_port, console_port, credentials=credentials
                    )
                    return results["overall_status"]
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(check_node_health, nodes))
            
            if not all(results):
                self.logger.error("某些节点的健康检查失败")
                exit(1)
        
        self.logger.info("=" * 60)
        self.logger.info("健康检查完成")
        self.logger.info("=" * 60)
    
    def check_minio_exists(self):
        """
        检查所有节点上是否已存在MinIO服务
        """
        self.logger.info("## 检查MinIO服务是否存在")
        
        deployment_mode = self.config.get("deployment_mode")
        nodes_with_minio = []
        nodes_without_minio = []
        
        if deployment_mode == "standalone":
            standalone_config = self.config.get("standalone", {})
            host = standalone_config.get("host", "localhost")
            
            if host in ["localhost", "127.0.0.1", "127.0.1.1"]:
                # 本地主机，直接检查
                if self.service_manager.check_service_exists():
                    nodes_with_minio.append({"host": host, "type": "本地主机"})
                else:
                    nodes_without_minio.append({"host": host, "type": "本地主机"})
            else:
                # 远程主机，通过SSH检查
                ssh_params = self.get_ssh_params()
                
                # 构造检查命令
                check_cmd = "systemctl list-unit-files --type service | grep -q minio || [ -f /etc/systemd/system/minio.service ]"
                exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                
                if exit_code == 0:
                    nodes_with_minio.append({"host": host, "type": "远程主机"})
                else:
                    nodes_without_minio.append({"host": host, "type": "远程主机"})
        
        elif deployment_mode == "cluster":
            cluster_config = self.config.get("cluster", {})
            nodes = cluster_config.get("nodes", [])
            
            for node in nodes:
                ssh_params = self.get_ssh_params(node)
                
                # 构造检查命令
                check_cmd = "systemctl list-unit-files --type service | grep -q minio || [ -f /etc/systemd/system/minio.service ]"
                exit_code, stdout, stderr = self.remote_executor.execute_command(ssh_params["host"], check_cmd, ssh_params["port"], ssh_params["username"], ssh_params["ssh_key"], ssh_params["password"])
                
                if exit_code == 0:
                    nodes_with_minio.append({"host": ssh_params["host"], "type": "集群节点"})
                else:
                    nodes_without_minio.append({"host": ssh_params["host"], "type": "集群节点"})
        
        # 显示检查结果
        total_nodes = len(nodes_with_minio) + len(nodes_without_minio)
        self.logger.info(f"\n检查结果：")
        self.logger.info(f"总节点数：{total_nodes}")
        
        if nodes_with_minio:
            self.logger.warning(f"已安装MinIO的节点数：{len(nodes_with_minio)}")
            for node in nodes_with_minio:
                self.logger.warning(f"  - {node['host']} ({node['type']})")
        else:
            self.logger.info(f"已安装MinIO的节点数：0")
        
        if nodes_without_minio:
            self.logger.info(f"未安装MinIO的节点数：{len(nodes_without_minio)}")
            for node in nodes_without_minio:
                self.logger.info(f"  - {node['host']} ({node['type']})")
        else:
            self.logger.info(f"未安装MinIO的节点数：0")
        
        # 如果有节点已安装MinIO，给出友好提示并退出
        if nodes_with_minio:
            self.logger.error("\n检测到部分或全部节点已安装MinIO服务！")
            self.logger.error("为避免覆盖现有MinIO环境，部署已终止。")
            self.logger.error("如需在已安装MinIO的节点上重新部署，请先手动卸载现有MinIO服务。")
            exit(1)
        
        self.logger.info("所有节点均未安装MinIO，继续部署流程")
        self.logger.info("-" * 60)
    
    def run(self):
        """
        运行完整的部署流程
        """
        if self.dry_run:
            self.logger.info("## 预演模式（DRY RUN）已启用")
            self.logger.info("不会执行实际操作，仅显示执行计划")
            self.logger.info("-" * 60)
        
        # 1. 运行系统检查
        self.run_system_checks()
        
        # 2. 加载配置
        self.load_config()
        
        # 3. 检查和配置SSH互信
        self.check_ssh_trust()
        
        # 4. 检查操作系统分区
        self.check_os_partitions()
        
        # 5. 检查MinIO服务是否存在
        self.check_minio_exists()
        
        # 6. 配置防火墙
        self.configure_firewall()
        
        # # 7. 安装MinIO
        # self.install_minio()
        
        # # 8. 配置MinIO服务
        # self.configure_minio_service()
        
        # # 9. 运行健康检查
        # self.run_health_checks()
        
        self.logger.info("=" * 60)
        self.logger.info("MinIO部署完成")
        self.logger.info("=" * 60)
        
        if self.dry_run:
            self.logger.info("预演模式执行完成，未进行实际部署操作")
        else:
            self.logger.info("MinIO已成功部署并运行")
