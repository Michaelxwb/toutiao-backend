from flask import request, g

from .jwt_util import verify_jwt


# @app.before_request
def jwt_authorization():
    '''
    每次登录之前进行jwt用户认证
    :return:
    '''
    g.user_id = None
    g.is_refresh = False
    # 验证请求头中的token中的user_id
    header_token = request.headers.get('Authorization')
    if header_token and header_token.startswith('Bearer '):
        token = header_token[7:]
        payload = verify_jwt(token)
        if payload:
            g.user_id = payload.get('user_id')
            g.is_refresh = payload.get('is_refresh', False)
