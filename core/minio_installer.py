import os
import platform
import subprocess
from core.logger import Logger

class MinioInstaller:
    def __init__(self, logger=None):
        self.logger = logger or Logger().get_logger()
        self.system_arch = self._get_system_arch()
    
    def _get_system_arch(self):
        """
        获取系统架构
        
        Returns:
            str: 系统架构，如 "amd64", "arm64" 等
        """
        arch = platform.machine()
        if arch == "x86_64":
            return "amd64"
        elif arch in ["aarch64", "arm64"]:
            return "arm64"
        else:
            return arch
    
    def download_file(self, url, dest_path):
        """
        从指定URL下载文件
        
        Args:
            url: 下载URL
            dest_path: 目标文件路径
        
        Returns:
            bool: True表示下载成功，False表示失败
        """
        self.logger.info(f"从 {url} 下载文件到 {dest_path}")
        
        # 尝试使用curl下载
        try:
            cmd = f"curl -sSL {url} -o {dest_path}"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            self.logger.info(f"使用curl下载成功：{url}")
            return True
        except Exception as e:
            self.logger.error(f"使用curl下载失败：{url}，错误：{e}")
        
        # 尝试使用wget下载
        try:
            cmd = f"wget -q {url} -O {dest_path}"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            self.logger.info(f"使用wget下载成功：{url}")
            return True
        except Exception as e:
            self.logger.error(f"使用wget下载失败：{url}，错误：{e}")
        
        return False
    
    def check_file_compatibility(self, file_path):
        """
        检查本地文件与当前操作系统的兼容性
        
        Args:
            file_path: 本地文件路径
        
        Returns:
            bool: True表示兼容，False表示不兼容
        """
        self.logger.info(f"检查文件 {file_path} 与当前系统的兼容性...")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            self.logger.error(f"文件 {file_path} 不存在")
            return False
        
        # 检查文件是否为可执行文件
        if not os.access(file_path, os.X_OK):
            self.logger.warning(f"文件 {file_path} 不是可执行文件，尝试添加执行权限")
            try:
                os.chmod(file_path, 0o755)
                self.logger.info(f"已添加执行权限：{file_path}")
            except Exception as e:
                self.logger.error(f"添加执行权限失败：{file_path}，错误：{e}")
                return False
        
        # 检查文件架构兼容性
        try:
            result = subprocess.run(
                ["file", file_path],
                capture_output=True, text=True, check=True
            )
            file_output = result.stdout.lower()
            
            if self.system_arch == "amd64":
                if "x86-64" in file_output or "amd64" in file_output:
                    self.logger.info(f"文件 {file_path} 与当前系统架构 {self.system_arch} 兼容")
                    return True
            elif self.system_arch == "arm64":
                if "aarch64" in file_output or "arm64" in file_output:
                    self.logger.info(f"文件 {file_path} 与当前系统架构 {self.system_arch} 兼容")
                    return True
            else:
                # 对于其他架构，尝试直接运行
                self.logger.info(f"尝试运行文件 {file_path} 以检查兼容性")
                try:
                    result = subprocess.run(
                        [file_path, "--version"],
                        capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0 or "version" in result.stdout.lower() or "version" in result.stderr.lower():
                        self.logger.info(f"文件 {file_path} 与当前系统架构 {self.system_arch} 兼容")
                        return True
                except Exception as e:
                    self.logger.error(f"运行文件 {file_path} 失败：{e}")
            
            self.logger.error(f"文件 {file_path} 与当前系统架构 {self.system_arch} 不兼容，file输出：{file_output}")
            return False
            
        except Exception as e:
            self.logger.error(f"检查文件兼容性失败：{file_path}，错误：{e}")
            return False
    
    def install_minio(self, minio_config, install_dir="/usr/local/bin"):
        """
        安装MinIO服务器
        
        Args:
            minio_config: MinIO配置字典
            install_dir: 安装目录，默认为 /usr/local/bin
        
        Returns:
            bool: True表示安装成功，False表示失败
        """
        self.logger.info("开始安装MinIO服务器...")
        
        # 创建安装目录
        if not os.path.exists(install_dir):
            try:
                os.makedirs(install_dir)
                self.logger.info(f"创建安装目录：{install_dir}")
            except Exception as e:
                self.logger.error(f"创建安装目录失败：{install_dir}，错误：{e}")
                return False
        
        minio_version = minio_config.get("version", "RELEASE.2024-01-18T22-51-48Z")
        minio_url = minio_config.get("download_url", f"https://dl.min.io/server/minio/release/linux-{self.system_arch}/minio")
        local_package_dir = minio_config.get("local_package_dir", "packages")
        
        minio_dest = os.path.join(install_dir, "minio")
        
        # 优先从官网下载
        if self.download_file(minio_url, minio_dest):
            # 设置执行权限
            try:
                os.chmod(minio_dest, 0o755)
                self.logger.info(f"已设置MinIO执行权限：{minio_dest}")
            except Exception as e:
                self.logger.error(f"设置MinIO执行权限失败：{e}")
                return False
            
            self.logger.info(f"MinIO服务器安装成功：{minio_dest}")
            return True
        
        # 官网下载失败，尝试从本地packages目录安装
        self.logger.info("官网下载失败，尝试从本地packages目录安装")
        
        # 查找本地MinIO安装包
        local_minio_files = []
        if os.path.exists(local_package_dir):
            for file in os.listdir(local_package_dir):
                if "minio" in file.lower() and not file.endswith(".md5") and not file.endswith(".sha256"):
                    local_minio_files.append(os.path.join(local_package_dir, file))
        
        if not local_minio_files:
            self.logger.error(f"本地packages目录中未找到MinIO安装包：{local_package_dir}")
            return False
        
        # 尝试安装每个本地包，直到找到兼容的
        for local_file in local_minio_files:
            self.logger.info(f"尝试安装本地MinIO包：{local_file}")
            
            # 检查文件兼容性
            if not self.check_file_compatibility(local_file):
                self.logger.warning(f"本地包 {local_file} 与当前系统不兼容，跳过")
                continue
            
            # 复制文件到安装目录
            try:
                subprocess.run(
                    ["cp", local_file, minio_dest],
                    check=True, capture_output=True, text=True
                )
                # 设置执行权限
                os.chmod(minio_dest, 0o755)
                self.logger.info(f"MinIO服务器从本地包安装成功：{minio_dest}")
                return True
            except Exception as e:
                self.logger.error(f"安装本地MinIO包失败：{local_file}，错误：{e}")
                continue
        
        self.logger.error("所有本地MinIO包均不兼容当前系统")
        print("错误：无法从官网下载MinIO，且所有本地MinIO包均不兼容当前系统")
        return False
    
    def install_mc(self, minio_config, install_dir="/usr/local/bin"):
        """
        安装MinIO客户端(mc)
        
        Args:
            minio_config: MinIO配置字典
            install_dir: 安装目录，默认为 /usr/local/bin
        
        Returns:
            bool: True表示安装成功，False表示失败
        """
        self.logger.info("开始安装MinIO客户端(mc)...")
        
        # 创建安装目录
        if not os.path.exists(install_dir):
            try:
                os.makedirs(install_dir)
                self.logger.info(f"创建安装目录：{install_dir}")
            except Exception as e:
                self.logger.error(f"创建安装目录失败：{install_dir}，错误：{e}")
                return False
        
        mc_version = minio_config.get("mc_version", "RELEASE.2024-01-18T22-51-48Z")
        mc_url = minio_config.get("mc_download_url", f"https://dl.min.io/client/mc/release/linux-{self.system_arch}/mc")
        mc_local_package_dir = minio_config.get("mc_local_package_dir", "packages")
        
        mc_dest = os.path.join(install_dir, "mc")
        
        # 优先从官网下载
        if self.download_file(mc_url, mc_dest):
            # 设置执行权限
            try:
                os.chmod(mc_dest, 0o755)
                self.logger.info(f"已设置mc执行权限：{mc_dest}")
            except Exception as e:
                self.logger.error(f"设置mc执行权限失败：{e}")
                return False
            
            self.logger.info(f"MinIO客户端(mc)安装成功：{mc_dest}")
            return True
        
        # 官网下载失败，尝试从本地packages目录安装
        self.logger.info("官网下载失败，尝试从本地packages目录安装mc")
        
        # 查找本地mc安装包
        local_mc_files = []
        if os.path.exists(mc_local_package_dir):
            for file in os.listdir(mc_local_package_dir):
                if "mc" in file.lower() and not file.endswith(".md5") and not file.endswith(".sha256"):
                    local_mc_files.append(os.path.join(mc_local_package_dir, file))
        
        if not local_mc_files:
            self.logger.error(f"本地packages目录中未找到mc安装包：{mc_local_package_dir}")
            return False
        
        # 尝试安装每个本地包，直到找到兼容的
        for local_file in local_mc_files:
            self.logger.info(f"尝试安装本地mc包：{local_file}")
            
            # 检查文件兼容性
            if not self.check_file_compatibility(local_file):
                self.logger.warning(f"本地包 {local_file} 与当前系统不兼容，跳过")
                continue
            
            # 复制文件到安装目录
            try:
                subprocess.run(
                    ["cp", local_file, mc_dest],
                    check=True, capture_output=True, text=True
                )
                # 设置执行权限
                os.chmod(mc_dest, 0o755)
                self.logger.info(f"MinIO客户端(mc)从本地包安装成功：{mc_dest}")
                return True
            except Exception as e:
                self.logger.error(f"安装本地mc包失败：{local_file}，错误：{e}")
                continue
        
        self.logger.warning("所有本地mc包均不兼容当前系统，跳过mc安装")
        return True  # mc安装失败不影响主程序
    
    def verify_installation(self):
        """
        验证MinIO安装是否成功
        
        Returns:
            bool: True表示安装成功，False表示失败
        """
        self.logger.info("验证MinIO安装...")
        
        # 检查minio命令是否可用
        try:
            result = subprocess.run(
                ["minio", "--version"],
                capture_output=True, text=True, check=True
            )
            self.logger.info(f"MinIO版本：{result.stdout.strip()}")
            return True
        except Exception as e:
            self.logger.error(f"验证MinIO安装失败：{e}")
            return False
