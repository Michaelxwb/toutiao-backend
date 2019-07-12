from flask import g, current_app
from functools import wraps
from sqlalchemy.orm import load_only
from sqlalchemy.exc import SQLAlchemyError

from models import db


def set_db_to_read(func):
    """
    设置使用读数据库
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        db.session().set_to_read()
        return func(*args, **kwargs)

    return wrapper


def set_db_to_write(func):
    """
    设置使用写数据库
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        db.session().set_to_write()
        return func(*args, **kwargs)

    return wrapper


# 要求用户必须登录的装饰器实现
def login_required(func):
    def wrapper(*args, **kwargs):
        # 判断用户是否登录
        # user_id有值，表示登录成功，引导进入视图函数
        if g.user_id and g.is_refresh is False:
            return func(*args, **kwargs)
        else:
            # user_id没有值，未登录，返回401认证失败
            return {"message": "invalid token"}, 401

    return wrapper
