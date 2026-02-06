import os
import re
import subprocess
from core.logger import Logger

class DiskManager:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
    
    def get_os_partitions(self):
        """
        获取操作系统关键分区列表
        
        Returns:
            list: 操作系统关键分区列表，每个元素是(设备路径, 挂载点)
        """
        os_partitions = []
        
        try:
            with open('/proc/mounts', 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) < 2:
                        continue
                    
                    dev = parts[0]
                    mount_point = parts[1]
                    
                    # 关键分区：根分区、boot分区等
                    if mount_point in ['/', '/boot', '/boot/efi']:
                        os_partitions.append((dev, mount_point))
            
            self.logger.info(f"操作系统关键分区：{os_partitions}")
            return os_partitions
            
        except Exception as e:
            self.logger.error(f"获取操作系统分区失败：{e}")
            return []
    
    def get_os_disks(self):
        """
        获取操作系统磁盘列表
        
        Returns:
            set: 操作系统磁盘集合，如 {'/dev/sda', '/dev/vda'}
        """
        os_partitions = self.get_os_partitions()
        os_disks = set()
        
        for part, mount_point in os_partitions:
            # 提取磁盘设备（如 /dev/sda1 -> /dev/sda）
            disk_match = re.match(r'(/dev/[a-zA-Z]+)', part)
            if disk_match:
                os_disks.add(disk_match.group(1))
        
        self.logger.info(f"操作系统磁盘列表：{os_disks}")
        return os_disks
    
    def check_partition_exists(self, device):
        """
        检查指定的分区是否存在
        
        Args:
            device: 设备路径（如 /dev/sdb1）
        
        Returns:
            bool: True如果分区存在，False如果不存在
        """
        self.logger.info(f"检查设备 {device} 是否存在...")
        
        # 直接检查设备文件是否存在
        if os.path.exists(device):
            self.logger.info(f"设备 {device} 存在")
            return True
        else:
            self.logger.error(f"设备 {device} 不存在")
            print(f"错误：设备 {device} 不存在")
            return False
    
    def check_os_partition(self, device):
        """
        检查指定设备是否为操作系统分区
        
        Args:
            device: 设备路径（如 /dev/sdb1）
        
        Returns:
            bool: True如果不是操作系统分区，False如果是
        """
        self.logger.info(f"检查设备 {device} 是否为操作系统分区...")
        
        # 获取操作系统关键分区
        os_partitions = self.get_os_partitions()
        os_partition_devices = [part[0] for part in os_partitions]
        
        # 检查设备是否为操作系统分区
        if device in os_partition_devices:
            for part, mount_point in os_partitions:
                if part == device:
                    self.logger.error(f"检测到设备 {device} 是操作系统分区（{mount_point}），不能用于MinIO存储")
                    print(f"错误：设备 {device} 是操作系统分区（{mount_point}），不能用于MinIO存储")
                    return False
        
        # 检查设备是否为操作系统磁盘（如 /dev/sda，而不仅仅是 /dev/sda1）
        os_disks = self.get_os_disks()
        if device in os_disks:
            self.logger.error(f"检测到设备 {device} 是操作系统磁盘，包含关键分区")
            print(f"错误：设备 {device} 是操作系统磁盘，包含关键分区")
            return False
        
        self.logger.info(f"设备 {device} 不是操作系统分区，可以安全使用")
        return True
    
    def check_disk_space(self, path, min_space_gb=10):
        """
        检查磁盘空间是否足够
        
        Args:
            path: 要检查的路径
            min_space_gb: 最小可用空间（GB），默认为10GB
        
        Returns:
            bool: True表示空间足够，False表示空间不足
        """
        self.logger.info(f"检查路径 {path} 的磁盘空间，需要至少 {min_space_gb}GB")
        
        try:
            stat = os.statvfs(path)
            # 计算可用空间（GB）
            available_space_gb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024 * 1024)
            
            self.logger.info(f"路径 {path} 的可用空间：{available_space_gb:.2f}GB")
            
            if available_space_gb >= min_space_gb:
                self.logger.info(f"磁盘空间足够：{available_space_gb:.2f}GB >= {min_space_gb}GB")
                return True
            else:
                self.logger.error(f"磁盘空间不足：{available_space_gb:.2f}GB < {min_space_gb}GB")
                print(f"错误：路径 {path} 的磁盘空间不足，可用空间：{available_space_gb:.2f}GB，需要至少 {min_space_gb}GB")
                return False
                
        except Exception as e:
            self.logger.error(f"检查磁盘空间失败：{e}")
            return False
    
    def format_disk(self, device, filesystem='ext4'):
        """
        格式化磁盘
        
        Args:
            device: 设备路径（如 /dev/sdb1）
            filesystem: 文件系统类型，默认为ext4
        
        Returns:
            bool: True表示格式化成功，False表示失败
        """
        self.logger.info(f"格式化设备 {device} 为 {filesystem} 文件系统")
        
        try:
            # 检查设备是否存在
            if not os.path.exists(device):
                self.logger.error(f"设备 {device} 不存在")
                return False
            
            # 使用mkfs格式化磁盘，添加-f参数强制格式化
            cmd = f"mkfs.{filesystem} -f {device}"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            
            self.logger.info(f"设备 {device} 格式化成功，输出：{result.stdout}")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"设备 {device} 格式化失败，错误：{e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"设备 {device} 格式化失败，错误：{e}")
            return False
    
    def mount_disk(self, device, mount_point):
        """
        挂载磁盘
        
        Args:
            device: 设备路径（如 /dev/sdb1）
            mount_point: 挂载点路径（如 /data/minio）
        
        Returns:
            bool: True表示挂载成功，False表示失败
        """
        self.logger.info(f"挂载设备 {device} 到 {mount_point}")
        
        try:
            # 检查挂载点是否存在，如果不存在则创建
            if not os.path.exists(mount_point):
                self.logger.info(f"创建挂载点 {mount_point}")
                os.makedirs(mount_point, exist_ok=True)
            
            # 检查是否已挂载
            with open('/proc/mounts', 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) < 2:
                        continue
                    if parts[0] == device and parts[1] == mount_point:
                        self.logger.info(f"设备 {device} 已挂载到 {mount_point}")
                        return True
            
            # 执行挂载命令
            cmd = f"mount {device} {mount_point}"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            
            self.logger.info(f"设备 {device} 挂载成功")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"设备 {device} 挂载失败，错误：{e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"设备 {device} 挂载失败，错误：{e}")
            return False
    
    def add_to_fstab(self, device, mount_point, filesystem='ext4', options='defaults'):
        """
        将挂载配置添加到fstab，实现持久化挂载
        
        Args:
            device: 设备路径（如 /dev/sdb1）
            mount_point: 挂载点路径（如 /data/minio）
            filesystem: 文件系统类型，默认为ext4
            options: 挂载选项，默认为defaults
        
        Returns:
            bool: True表示添加成功，False表示失败
        """
        self.logger.info(f"将设备 {device} 的挂载配置添加到fstab")
        
        try:
            # 检查是否已存在于fstab
            with open('/etc/fstab', 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split()
                    if len(parts) < 6:
                        continue
                    if parts[0] == device and parts[1] == mount_point:
                        self.logger.info(f"设备 {device} 的挂载配置已存在于fstab")
                        return True
            
            # 生成fstab条目
            fstab_entry = f"{device} {mount_point} {filesystem} {options} 0 2\n"
            
            # 添加到fstab
            with open('/etc/fstab', 'a') as f:
                f.write(fstab_entry)
            
            self.logger.info(f"设备 {device} 的挂载配置已添加到fstab")
            return True
            
        except Exception as e:
            self.logger.error(f"将设备 {device} 的挂载配置添加到fstab失败，错误：{e}")
            return False
    
    def set_permissions(self, path, user='root', group='root', mode='0755'):
        """
        设置路径权限
        
        Args:
            path: 路径
            user: 所有者，默认为root
            group: 所属组，默认为root
            mode: 权限模式，默认为0755
        
        Returns:
            bool: True表示设置成功，False表示失败
        """
        self.logger.info(f"设置路径 {path} 的权限：所有者 {user}，所属组 {group}，权限 {mode}")
        
        try:
            # 设置所有者和所属组
            cmd = f"chown -R {user}:{group} {path}"
            subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            
            # 设置权限
            cmd = f"chmod -R {mode} {path}"
            subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            
            self.logger.info(f"路径 {path} 的权限设置成功")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"设置路径 {path} 的权限失败，错误：{e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"设置路径 {path} 的权限失败，错误：{e}")
            return False
    
    def check_disk(self, device):
        """
        检查磁盘是否可用
        
        Args:
            device: 设备路径（如 /dev/sdb1）
        
        Returns:
            bool: True表示磁盘可用，False表示不可用
        """
        self.logger.info(f"检查设备 {device} 是否可用")
        
        try:
            # 检查设备是否存在
            if not os.path.exists(device):
                self.logger.error(f"设备 {device} 不存在")
                return False
            
            # 检查设备是否可访问
            cmd = f"lsblk {device}"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            
            self.logger.info(f"设备 {device} 可用，信息：{result.stdout}")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"设备 {device} 不可用，错误：{e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"设备 {device} 检查失败，错误：{e}")
            return False
    
    def prepare_disk(self, device, mount_point, filesystem='ext4', format_disk=False, min_space_gb=10):
        """
        准备磁盘：检查、格式化、挂载、设置权限
        
        Args:
            device: 设备路径（如 /dev/sdb1）
            mount_point: 挂载点路径（如 /data/minio）
            filesystem: 文件系统类型，默认为ext4
            format_disk: 是否格式化磁盘，默认为False
            min_space_gb: 最小可用空间（GB），默认为10GB
        
        Returns:
            bool: True表示准备成功，False表示失败
        """
        self.logger.info(f"准备磁盘：设备 {device}，挂载点 {mount_point}")
        
        # 检查是否为操作系统分区
        if not self.check_os_partition(device):
            return False
        
        # 检查磁盘是否可用
        if not self.check_disk(device):
            return False
        
        # 格式化磁盘（如果需要）
        if format_disk:
            if not self.format_disk(device, filesystem):
                return False
        
        # 挂载磁盘
        if not self.mount_disk(device, mount_point):
            return False
        
        # 添加到fstab
        if not self.add_to_fstab(device, mount_point, filesystem):
            return False
        
        # 检查磁盘空间
        if not self.check_disk_space(mount_point, min_space_gb):
            return False
        
        # 设置权限
        if not self.set_permissions(mount_point):
            return False
        
        self.logger.info(f"磁盘 {device} 准备成功，已挂载到 {mount_point}")
        return True
