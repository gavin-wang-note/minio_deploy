import subprocess
from core.logger import Logger

class FirewallManager:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
        self.firewall_type = None
    
    def detect_firewall_type(self):
        """
        检测系统使用的防火墙类型
        
        Returns:
            str: 防火墙类型，可能是 "firewalld"、"iptables" 或 "unknown"
        """
        self.logger.info("检测防火墙类型...")
        
        # 检查firewalld
        try:
            result = subprocess.run(["which", "firewalld"], capture_output=True, text=True)
            if result.returncode == 0:
                self.logger.info("检测到防火墙类型：firewalld")
                self.firewall_type = "firewalld"
                return "firewalld"
        except Exception as e:
            self.logger.debug(f"检查firewalld失败：{e}")
        
        # 检查iptables
        try:
            result = subprocess.run(["which", "iptables"], capture_output=True, text=True)
            if result.returncode == 0:
                self.logger.info("检测到防火墙类型：iptables")
                self.firewall_type = "iptables"
                return "iptables"
        except Exception as e:
            self.logger.debug(f"检查iptables失败：{e}")
        
        self.logger.info("未检测到已知防火墙类型")
        self.firewall_type = "unknown"
        return "unknown"
    
    def is_firewall_running(self):
        """
        检查防火墙是否运行
        
        Returns:
            bool: True表示防火墙正在运行，False表示未运行或未知
        """
        # 确保已检测防火墙类型
        if self.firewall_type is None:
            self.detect_firewall_type()
        
        self.logger.info(f"检查{self.firewall_type}是否运行...")
        
        if self.firewall_type == "firewalld":
            try:
                result = subprocess.run(
                    ["systemctl", "is-active", "firewalld"],
                    capture_output=True, text=True
                )
                return result.stdout.strip() == "active" and result.returncode == 0
            except Exception as e:
                self.logger.debug(f"检查firewalld状态失败：{e}")
                return False
        
        elif self.firewall_type == "iptables":
            try:
                # 检查iptables规则是否存在
                result = subprocess.run(
                    ["iptables", "-L", "-n"],
                    capture_output=True, text=True, check=True
                )
                return True
            except Exception as e:
                self.logger.debug(f"检查iptables状态失败：{e}")
                return False
        
        self.logger.info("未知防火墙类型，无法检查状态")
        return False
    
    def check_port_open(self, port, protocol="tcp"):
        """
        检查指定端口是否已开放
        
        Args:
            port: 端口号
            protocol: 协议类型，默认为tcp
        
        Returns:
            bool: True表示端口已开放，False表示未开放或无法检测
        """
        # 确保已检测防火墙类型
        if self.firewall_type is None:
            self.detect_firewall_type()
        
        self.logger.info(f"检查端口 {port}/{protocol} 是否已开放...")
        
        if self.firewall_type == "firewalld":
            try:
                result = subprocess.run(
                    ["firewall-cmd", "--list-ports"],
                    capture_output=True, text=True, check=True
                )
                ports = result.stdout.strip().split()
                target_port = f"{port}/{protocol}"
                if target_port in ports:
                    self.logger.info(f"端口 {port}/{protocol} 已开放（firewalld）")
                    return True
                
                # 检查是否在某个zone中开放
                result = subprocess.run(
                    ["firewall-cmd", "--get-active-zones"],
                    capture_output=True, text=True, check=True
                )
                zones = []
                for line in result.stdout.strip().split("\n"):
                    if not line or "interfaces:" in line:
                        continue
                    zones.append(line.strip())
                
                for zone in zones:
                    result = subprocess.run(
                        ["firewall-cmd", "--zone", zone, "--list-ports"],
                        capture_output=True, text=True, check=True
                    )
                    ports = result.stdout.strip().split()
                    if target_port in ports:
                        self.logger.info(f"端口 {port}/{protocol} 已在zone {zone} 中开放（firewalld）")
                        return True
                
                self.logger.info(f"端口 {port}/{protocol} 未开放（firewalld）")
                return False
            except Exception as e:
                self.logger.error(f"检查firewalld端口失败：{e}")
                return False
        
        elif self.firewall_type == "iptables":
            try:
                result = subprocess.run(
                    ["iptables", "-L", "INPUT", "-n", "-v", "--line-numbers"],
                    capture_output=True, text=True, check=True
                )
                
                for line in result.stdout.strip().split("\n"):
                    if f"{protocol.upper()}" in line and f"dpt:{port}" in line and "ACCEPT" in line:
                        self.logger.info(f"端口 {port}/{protocol} 已开放（iptables）")
                        return True
                
                self.logger.info(f"端口 {port}/{protocol} 未开放（iptables）")
                return False
            except Exception as e:
                self.logger.error(f"检查iptables端口失败：{e}")
                return False
        
        self.logger.info(f"未知防火墙类型，无法检查端口 {port}/{protocol}")
        return False
    
    def open_port(self, port, protocol="tcp", permanent=True):
        """
        开放指定端口
        
        Args:
            port: 端口号
            protocol: 协议类型，默认为tcp
            permanent: 是否持久化配置，默认为True
        
        Returns:
            bool: True表示端口开放成功，False表示失败
        """
        # 确保已检测防火墙类型
        if self.firewall_type is None:
            self.detect_firewall_type()
        self.logger.info(f"开放端口 {port}/{protocol}...")

        if self.firewall_type == "firewalld":
            try:
                # 临时开放端口
                subprocess.run(
                    ["firewall-cmd", "--add-port", f"{port}/{protocol}"],
                    capture_output=True, text=True, check=True
                )

                if permanent:
                    # 持久化配置
                    subprocess.run(
                        ["firewall-cmd", "--permanent", "--add-port", f"{port}/{protocol}"],
                        capture_output=True, text=True, check=True
                    )

                self.logger.info(f"端口 {port}/{protocol} 开放成功（firewalld）")
                return True
            except Exception as e:
                self.logger.error(f"开放firewalld端口失败：{e}")
                return False
        elif self.firewall_type == "iptables":
            try:
                # 添加iptables规则
                subprocess.run(
                    ["iptables", "-A", "INPUT", "-p", protocol, "--dport", str(port), "-j", "ACCEPT"],
                    capture_output=True, text=True, check=True
                )

                if permanent:
                    # 保存iptables规则
                    if self._is_redhat_based():
                        # RHEL/CentOS系统
                        subprocess.run(
                            ["service", "iptables", "save"],
                            capture_output=True, text=True, check=True
                        )
                    else:
                        # Debian/Ubuntu系统
                        subprocess.run(
                            ["iptables-save", ">", "/etc/iptables/rules.v4"],
                            shell=True, capture_output=True, text=True, check=True
                        )
                
                self.logger.info(f"端口 {port}/{protocol} 开放成功（iptables）")
                return True
            except Exception as e:
                self.logger.error(f"开放iptables端口失败：{e}")
                return False
        
        self.logger.error(f"未知防火墙类型，无法开放端口 {port}/{protocol}")
        return False
    
    def _is_redhat_based(self):
        """
        检测是否为RedHat-based系统
        
        Returns:
            bool: True表示是RedHat-based系统，False表示不是
        """
        try:
            # 检查是否存在redhat-release文件
            result = subprocess.run(
                ["ls", "/etc/redhat-release"],
                capture_output=True, text=True, check=True
            )
            return True
        except Exception:
            # 检查是否存在centos-release文件
            try:
                result = subprocess.run(
                    ["ls", "/etc/centos-release"],
                    capture_output=True, text=True, check=True
                )
                return True
            except Exception:
                return False
    
    def configure_firewall(self, ports, protocol="tcp", permanent=True):
        """
        配置防火墙，开放指定的端口列表
        
        Args:
            ports: 端口列表，如 [9000, 9001]
            protocol: 协议类型，默认为tcp
            permanent: 是否持久化配置，默认为True
        
        Returns:
            bool: True表示所有端口配置成功，False表示至少有一个端口配置失败
        """
        # 确保已检测防火墙类型
        if self.firewall_type is None:
            self.detect_firewall_type()
        self.logger.info(f"配置防火墙，开放端口：{ports}/{protocol}")
        
        # 如果防火墙未运行，直接返回成功
        if not self.is_firewall_running():
            self.logger.info("防火墙未运行，跳过端口开放")
            return True
        
        success = True
        
        for port in ports:
            # 检查端口是否已开放
            if self.check_port_open(port, protocol):
                self.logger.info(f"端口 {port}/{protocol} 已开放，跳过配置")
                continue
            
            # 开放端口
            if not self.open_port(port, protocol, permanent):
                success = False
        
        return success
