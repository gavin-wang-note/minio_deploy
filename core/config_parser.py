import yaml
import os
from core.logger import Logger

class ConfigParser:
    def __init__(self, config_file, logger=None):
        self.config_file = config_file
        self.logger = logger or Logger().get_logger()
        self.config = None
    
    def load_config(self):
        """
        加载并解析YAML配置文件
        """
        self.logger.info(f"加载配置文件：{self.config_file}")
        
        if not os.path.exists(self.config_file):
            self.logger.error(f"配置文件 {self.config_file} 不存在")
            print(f"错误：配置文件 {self.config_file} 不存在")
            exit(1)
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            self.logger.info("配置文件加载成功")
            return self.config
        
        except yaml.YAMLError as e:
            self.logger.error(f"配置文件解析失败：{e}")
            print(f"错误：配置文件解析失败：{e}")
            exit(1)
        except Exception as e:
            self.logger.error(f"加载配置文件失败：{e}")
            print(f"错误：加载配置文件失败：{e}")
            exit(1)
    
    def validate_config(self):
        """
        验证配置的有效性
        """
        if not self.config:
            self.load_config()
        
        self.logger.info("开始验证配置有效性...")
        
        # 验证部署模式
        deployment_mode = self.config.get('deployment_mode')
        if not deployment_mode:
            self.logger.error("缺少部署模式配置：deployment_mode")
            exit(1)
        
        if deployment_mode not in ['standalone', 'cluster']:
            self.logger.error(f"无效的部署模式：{deployment_mode}，仅支持 standalone 或 cluster")
            exit(1)
        
        # 验证MinIO配置
        if not self.config.get('minio'):
            self.logger.error("缺少MinIO配置：minio")
            exit(1)
        
        # 验证认证信息
        if not self.config.get('credentials'):
            self.logger.error("缺少认证信息配置：credentials")
            exit(1)
        
        if not self.config['credentials'].get('root_user'):
            self.logger.error("缺少root_user配置")
            exit(1)
        
        if not self.config['credentials'].get('root_password'):
            self.logger.error("缺少root_password配置")
            exit(1)
        
        # 根据部署模式验证配置
        if deployment_mode == 'standalone':
            self._validate_standalone_config()
        else:
            self._validate_cluster_config()
        
        self.logger.info("配置验证通过")
        return True
    
    def _validate_standalone_config(self):
        """
        验证单机模式配置
        """
        if not self.config.get('standalone'):
            self.logger.error("缺少单机模式配置：standalone")
            exit(1)
        
        standalone_config = self.config['standalone']
        
        # 验证数据目录
        if not standalone_config.get('data_dir'):
            self.logger.error("缺少数据目录配置：standalone.data_dir")
            exit(1)
        
        # 验证端口配置 - 从cluster配置中获取，确保cluster配置存在且包含端口信息
        if not self.config.get('cluster'):
            self.logger.error("缺少集群配置：cluster")
            exit(1)
            
        cluster_config = self.config['cluster']
        if not cluster_config.get('server_port'):
            self.logger.error("缺少服务端口配置：cluster.server_port")
            exit(1)
            
        if not cluster_config.get('console_port'):
            self.logger.error("缺少控制台端口配置：cluster.console_port")
            exit(1)
            
        # 验证磁盘配置（必选项）
        if not standalone_config.get('disk'):
            self.logger.error("缺少磁盘配置：standalone.disk")
            exit(1)
        
        disk_config = standalone_config['disk']
        
        # 必须启用磁盘管理
        if not disk_config.get('enabled', False):
            self.logger.error("必须启用磁盘管理：standalone.disk.enabled = true")
            exit(1)
        
        # 必须指定设备路径
        if not disk_config.get('device'):
            self.logger.error("缺少磁盘设备路径：standalone.disk.device")
            exit(1)
        
        # 必须指定挂载点
        if not disk_config.get('mount_point'):
            self.logger.error("缺少磁盘挂载点：standalone.disk.mount_point")
            exit(1)
    
    def _validate_cluster_config(self):
        """
        验证集群模式配置
        """
        if not self.config.get('cluster'):
            self.logger.error("缺少集群模式配置：cluster")
            exit(1)
        
        cluster_config = self.config['cluster']
        
        # 验证节点配置
        if not cluster_config.get('nodes'):
            self.logger.error("缺少节点配置：cluster.nodes")
            exit(1)
        
        if len(cluster_config['nodes']) < 1:
            self.logger.error("集群模式至少需要一个节点")
            exit(1)
        
        # 验证每个节点的配置
        for i, node in enumerate(cluster_config['nodes']):
            if not node.get('host'):
                self.logger.error(f"节点 {i+1} 缺少host配置")
                exit(1)
            
            if not node.get('ip'):
                self.logger.error(f"节点 {i+1} 缺少ip配置")
                exit(1)
            
            if not node.get('data_dir'):
                self.logger.error(f"节点 {i+1} 缺少data_dir配置")
                exit(1)
            
            # 验证磁盘配置（必选项）
            if not node.get('disk'):
                self.logger.error(f"节点 {i+1}（{node['ip']}）缺少磁盘配置：disk")
                exit(1)
            
            disk_config = node['disk']
            
            # 必须启用磁盘管理
            if not disk_config.get('enabled', False):
                self.logger.error(f"节点 {i+1}（{node['ip']}）必须启用磁盘管理：disk.enabled = true")
                exit(1)
            
            # 必须指定设备路径
            if not disk_config.get('device'):
                self.logger.error(f"节点 {i+1}（{node['ip']}）缺少磁盘设备路径：disk.device")
                exit(1)
            
            # 必须指定挂载点
            if not disk_config.get('mount_point'):
                self.logger.error(f"节点 {i+1}（{node['ip']}）缺少磁盘挂载点：disk.mount_point")
                exit(1)
        
        # 验证端口配置
        if not cluster_config.get('server_port'):
            self.logger.error("缺少服务器端口配置：cluster.server_port")
            exit(1)
        
        if not cluster_config.get('console_port'):
            self.logger.error("缺少控制台端口配置：cluster.console_port")
            exit(1)
    
    def get_config(self, validate=True):
        """
        获取配置对象
        
        Args:
            validate: 是否验证配置，默认为True
        """
        if not self.config:
            self.load_config()
            if validate:
                self.validate_config()
        
        return self.config
        
    def validate_config(self, mode=None):
        """
        验证配置的有效性
        
        Args:
            mode: 部署模式，优先使用传入的模式，否则从配置中获取
        """
        if not self.config:
            self.load_config()
        
        self.logger.info("开始验证配置有效性...")
        
        # 使用传入的模式或从配置中获取
        deployment_mode = mode or self.config.get('deployment_mode')
        if not deployment_mode:
            self.logger.error("缺少部署模式配置：deployment_mode")
            exit(1)
        
        if deployment_mode not in ['standalone', 'cluster']:
            self.logger.error(f"无效的部署模式：{deployment_mode}，仅支持 standalone 或 cluster")
            exit(1)
        
        # 验证MinIO配置
        if not self.config.get('minio'):
            self.logger.error("缺少MinIO配置：minio")
            exit(1)
        
        # 验证认证信息
        if not self.config.get('credentials'):
            self.logger.error("缺少认证信息配置：credentials")
            exit(1)
        
        if not self.config['credentials'].get('root_user'):
            self.logger.error("缺少root_user配置")
            exit(1)
        
        if not self.config['credentials'].get('root_password'):
            self.logger.error("缺少root_password配置")
            exit(1)
        
        # 根据部署模式验证配置
        if deployment_mode == 'standalone':
            self._validate_standalone_config()
        else:
            self._validate_cluster_config()
        
        self.logger.info("配置验证通过")
        return True
    
    def get_deployment_mode(self):
        """
        获取部署模式
        """
        return self.get_config().get('deployment_mode')
    
    def get_minio_config(self):
        """
        获取MinIO配置
        """
        return self.get_config().get('minio')
    
    def get_credentials(self):
        """
        获取认证信息
        """
        return self.get_config().get('credentials')
    
    def get_standalone_config(self):
        """
        获取单机模式配置
        """
        if self.get_deployment_mode() != 'standalone':
            self.logger.error("当前部署模式不是单机模式")
            return None
        
        return self.get_config().get('standalone')
    
    def get_cluster_config(self):
        """
        获取集群模式配置
        """
        if self.get_deployment_mode() != 'cluster':
            self.logger.error("当前部署模式不是集群模式")
            return None
        
        return self.get_config().get('cluster')
    
    def get_advanced_config(self):
        """
        获取高级配置
        """
        return self.get_config().get('advanced', {})
