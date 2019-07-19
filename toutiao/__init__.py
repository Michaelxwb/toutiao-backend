from flask import Flask
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError
import grpc
from elasticsearch5 import Elasticsearch


# import socketio


def create_flask_app(config, enable_config_file=False):
    """
    创建Flask应用
    :param config: 配置信息对象
    :param enable_config_file: 是否允许运行环境中的配置文件覆盖已加载的配置信息
    :return: Flask应用
    """
    app = Flask(__name__)
    app.config.from_object(config)
    if enable_config_file:
        from utils import constants
        app.config.from_envvar(constants.GLOBAL_SETTING_ENV_NAME, silent=True)

    return app


def create_app(config, enable_config_file=False):
    """
    创建应用
    :param config: 配置信息对象
    :param enable_config_file: 是否允许运行环境中的配置文件覆盖已加载的配置信息
    :return: 应用
    """
    app = create_flask_app(config, enable_config_file)

    # 创建Snowflake ID worker
    from utils.snowflake.id_worker import IdWorker
    app.id_worker = IdWorker(app.config['DATACENTER_ID'],
                             app.config['WORKER_ID'],
                             app.config['SEQUENCE'])

    # 限流器
    from utils.limiter import limiter as lmt
    lmt.init_app(app)

    # 配置日志
    from utils.logging import create_logger
    create_logger(app)

    # 注册url转换器
    from utils.converters import register_converters
    register_converters(app)

    from redis.sentinel import Sentinel
    _sentinel = Sentinel(app.config['REDIS_SENTINELS'])
    app.redis_master = _sentinel.master_for(app.config['REDIS_SENTINEL_SERVICE_NAME'])
    app.redis_slave = _sentinel.slave_for(app.config['REDIS_SENTINEL_SERVICE_NAME'])

    from rediscluster import StrictRedisCluster
    app.redis_cluster = StrictRedisCluster(startup_nodes=app.config['REDIS_CLUSTER'])

    # rpc

    app.rpc_reco_channel = grpc.insecure_channel(app.config['RPC'].RECOMMEND)

    # app.rpc_reco = grpc.insecure_channel(app.config['RPC'].RECOMMEND)

    # Elasticsearch
    app.es = Elasticsearch(
        app.config['ES'],
        # sniff before doing anything
        sniff_on_start=True,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60
    )

    # socket.io
    # app.sio = socketio.KombuManager(app.config['RABBITMQ'], write_only=True)

    # MySQL数据库连接初始化
    from models import db

    db.init_app(app)

    # # 添加请求钩子
    from utils.middleware import jwt_authorization
    app.before_request(jwt_authorization)

    # 添加定时任务APScheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.executors.pool import ThreadPoolExecutor
    # 触发器
    from apscheduler.triggers import date, interval, cron
    from toutiao.schedule.statistics import fix_statistics

    # 1.创建执行器对象executors
    executors = {
        # 默认会将定时任务使用线程执行，并且添加到线程池，最大并发10个线程
        "default": ThreadPoolExecutor(max_workers=10)
    }

    # 2.创建调度器对象-使用executors进行配置
    scheduler = BackgroundScheduler(executors=executors)

    # 2.1 将scheduler对象保存到app中，其他地方如果需要添加`动态任务` :current_app.scheduler.add_job(动态任务)
    app.scheduler = scheduler

    # 3.添加任务--修正统计数据--`静态任务`
    # app.scheduler.add_job(func="定时任务函数引用", trigger="触发器", args=["参数"])
    # app.scheduler.add_job(func=fix_statistics, trigger=cron.CronTrigger(hour=4), args=["参数"])
    # 触发器凌晨4点执行任务
    # app.scheduler.add_job(func=fix_statistics, trigger="cron", hour=4, args=[app])
    app.scheduler.add_job(func=fix_statistics, trigger="date", args=[app])

    # 4.开启定时任务
    app.scheduler.start()

    # 注册用户模块蓝图
    from .resources.user import user_bp

    app.register_blueprint(user_bp)

    # 注册新闻模块蓝图
    from .resources.news import news_bp

    app.register_blueprint(news_bp)

    # 注册通知模块
    from .resources.notice import notice_bp

    app.register_blueprint(notice_bp)

    # 搜索
    from .resources.search import search_bp

    app.register_blueprint(search_bp)

    return app
