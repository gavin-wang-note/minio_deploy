#!/usr/bin/env python3
import argparse
import sys
from core.logger import Logger
from core.deployer import Deployer

def main():
    """
    主入口函数
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description="MinIO Linux部署工具",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 部署模式组
    mode_group = parser.add_argument_group('部署模式 (必填)')
    mode_group.add_argument("--mode", "-m", choices=["standalone", "cluster"], required=True, 
                        help="部署模式：standalone（单机模式）或cluster（集群模式）")
    
    # 配置组
    config_group = parser.add_argument_group('配置选项')
    config_group.add_argument("--config", "-c", default="config.yaml", 
                              help="配置文件路径，默认为当前目录下的config.yaml")
    config_group.add_argument("--dry-run", action="store_true", 
                              help="预演模式，只检查配置和显示执行计划，不执行实际部署操作")
    
    # 日志组
    log_group = parser.add_argument_group('日志选项')
    log_group.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
                        default="DEBUG", 
                        help="日志级别，默认为DEBUG，可选值：DEBUG、INFO、WARNING、ERROR、CRITICAL")
    
    # 添加示例说明
    parser.epilog = """
使用示例:
  单机部署: python deploy.py -m standalone -c config.yaml
  集群部署: python deploy.py -m cluster -c cluster_config.yaml
  预演模式: python deploy.py -m standalone -c config.yaml --dry-run
  调整日志: python deploy.py -m standalone --log-level INFO
    """
    
    args = parser.parse_args()
    
    # 初始化日志
    logger = Logger(log_level=args.log_level).get_logger()
    
    try:
        logger.info("# MinIO Linux部署工具")
        logger.info("-" * 60)
        
        # 创建部署器实例
        deployer = Deployer(args.config, args.dry_run, logger, args.mode)
        
        # 运行部署流程
        deployer.run()
        
        logger.info("部署流程执行完成")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.error("部署流程被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"部署流程执行失败：{e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
