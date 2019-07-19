from flask_restful import Resource
from flask_limiter.util import get_remote_address
from flask import request, current_app, g
from flask_restful.reqparse import RequestParser
import random
from datetime import datetime, timedelta
from redis.exceptions import ConnectionError

from celery_tasks.sms.tasks import send_verification_code
from . import constants
from utils import parser
from models import db
from models.user import User, UserProfile
from utils.jwt_util import generate_jwt
# from cache import user as cache_user
from utils.limiter import limiter as lmt
from utils.decorators import set_db_to_read, set_db_to_write, login_required


class SMSVerificationCodeResource(Resource):
    """
    短信验证码
    """
    error_message = 'Too many requests.'

    decorators = [
        lmt.limit(constants.LIMIT_SMS_VERIFICATION_CODE_BY_MOBILE,
                  key_func=lambda: request.view_args['mobile'],
                  error_message=error_message),
        lmt.limit(constants.LIMIT_SMS_VERIFICATION_CODE_BY_IP,
                  key_func=get_remote_address,
                  error_message=error_message)
    ]

    def get(self, mobile):
        code = '{:0>6d}'.format(random.randint(0, 999999))
        current_app.redis_master.setex('app:code:{}'.format(mobile), constants.SMS_VERIFICATION_CODE_EXPIRES, code)
        send_verification_code.delay(mobile, code)
        print('短信验证码：', code)
        return {'mobile': mobile}


class AuthorizationResource(Resource):
    """
    认证
    """
    method_decorators = {
        'post': [set_db_to_write],
        'put': [set_db_to_read],
        'get': [login_required]
    }

    def _generate_tokens(self, user_id: object, with_refresh_token: object = True) -> object:
        """
        生成token 和refresh_token
        :param user_id: 用户id
        :return: token, refresh_token
        """
        # 颁发JWT
        # 1、生成两小时时效jwt_token
        nowtime = datetime.utcnow()

        payload = {
            'user_id': user_id,
        }
        expiry_2h = nowtime + timedelta(hours=current_app.config['JWT_EXPIRY_HOURS'])

        secret = current_app.config['JWT_SECRET']

        token = generate_jwt(payload=payload, expiry=expiry_2h, secret=secret)

        # 2、生成14天时效refresh_token
        refresh_payload = {
            'user_id': user_id,
            'is_fresh': True
        }
        expiry_14d = nowtime + timedelta(days=current_app.config['JWT_REFRESH_DAYS'])

        refresh_token = generate_jwt(payload=refresh_payload, expiry=expiry_14d, secret=secret)

        return token, refresh_token

    def get(self):
        # 要求用户必须登录才能访问get
        return "get user info user_id:{}".format(g.user_id)

    def post(self):
        """
        登录创建token
        """
        json_parser = RequestParser()
        json_parser.add_argument('mobile', type=parser.mobile, required=True, location='json')
        json_parser.add_argument('code', type=parser.regex(r'^\d{6}$'), required=True, location='json')
        args = json_parser.parse_args()
        mobile = args.mobile
        code = args.code

        # 从redis中获取验证码
        key = 'app:code:{}'.format(mobile)
        try:
            real_code = current_app.redis_master.get(key)
        except ConnectionError as e:
            current_app.logger.error(e)
            real_code = current_app.redis_slave.get(key)

        try:
            current_app.redis_master.delete(key)
        except ConnectionError as e:
            current_app.logger.error(e)

        if not real_code or real_code.decode() != code:
            return {'message': 'Invalid code.'}, 400

        # 查询或保存用户
        user = User.query.filter_by(mobile=mobile).first()

        if user is None:
            # 用户不存在，注册用户
            user_id = current_app.id_worker.get_id()
            user = User(id=user_id, mobile=mobile, name=mobile, last_login=datetime.now())
            db.session.add(user)
            profile = UserProfile(id=user.id)
            db.session.add(profile)
            db.session.commit()
        else:
            if user.status == User.STATUS.DISABLE:
                return {'message': 'Invalid user.'}, 403

        token, refresh_token = self._generate_tokens(user.id)

        return {'token': token, 'refresh_token': refresh_token}, 201

    def put(self):
        '''
        根据refresh_token获取token值
        :return: 2h时效token
        '''
        # 获取refresh_token内的payload标志值
        user_id = g.user_id
        is_refresh = g.is_refresh

        # 做校验
        if user_id and is_refresh is True:
            token, refres_token = self._generate_tokens(user_id)

            return {'token': token}

        return {"message": "refresh token invalid"}, 403


class ModifiyResource(Resource):
    '''
    修改密码接口，确保用户在某端上修改密码之后，其他端的token也失效
    '''
    # 要求用户必须登录
    method_decorators = [login_required]

    # 生成新的token
    def _generate_tokens(self, user_id, with_refresh_token=True):
        """
        生成token 和refresh_token
        :param user_id: 用户id
        :return: token, refresh_token
        """
        # 颁发JWT

        # 1.用户身份认证的2小时有效的token
        # 构建用户载荷数据
        payload = {
            # 用户信息
            "user_id": user_id,
        }

        # 2小时的过期时长
        # 当前时间的标准时间戳utcnow()
        # expiry_2h = datetime.utcnow() + 不能使用2小时（7200秒）

        # 具体指明什么时候到期  now + 2小时的间隔时间  == 过期具体时间
        expiry_2h = datetime.utcnow() + timedelta(hours=current_app.config["JWT_EXPIRY_HOURS"])

        # 构建加密秘钥
        secret = current_app.config["JWT_SECRET"]

        token = generate_jwt(payload=payload, expiry=expiry_2h, secret=secret)

        # 2.刷新token 14天有效

        # 刷新token的payload
        refresh_payload = {
            "user_id": user_id,
            # 标识该载荷是刷新token
            "is_refresh": True
        }

        # 14天有效  当前的时间 + 14天间隔
        expiry_14d = datetime.utcnow() + timedelta(days=current_app.config["JWT_REFRESH_DAYS"])

        # 构建加密秘钥
        secret = current_app.config["JWT_SECRET"]

        # 生成刷新token
        refresh_token = generate_jwt(payload=refresh_payload, expiry=expiry_14d, secret=secret)

        return token, refresh_token

    def post(self):
        '''修改密码'''

        # 假设前端发了修改密码的请求
        user_id = g.user_id
        # 存到redis中白名单的key
        key = 'user:{}:token'.format(user_id)

        # 获取管道对象
        pl = current_app.redis_master.pipeline()

        # 生成新的new_token

        new_token, refresh_token = self._generate_tokens(user_id)

        pl.sadd(key, new_token)
        pl.expire(key, 7200)
        pl.execute()

        return {'message': 'Change password success！'}

    def get(self):
        '''模拟其他端登录'''
        user_id = g.user_id

        key = 'user:{}:token'.format(user_id)

        valid_tokens = current_app.redis_master.smembers(key)
        new_valid_tokens = [item.decode() for item in valid_tokens]

        # 判断前端传过来的token是否在new_valid_tokens中，不在重新登录

        header_token = request.headers.get("Authorization")
        token = header_token[7:]

        if valid_tokens and token not in new_valid_tokens:
            return {'message': 'Invalid token'}, 403
        else:
            return {'message': 'success'}, 200
