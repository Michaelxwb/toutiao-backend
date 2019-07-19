from flask import g, current_app
from flask_restful import Resource
from flask_restful.reqparse import RequestParser

from cache.persstorage import UserArticleCountStorage
from models import db
from models.user import User
from utils.decorators import login_required
from utils.parser import image_file
from utils.storage import pic_upload
from cache.user import UserProfileCache
from sqlalchemy.exc import DatabaseError


class PhotoResource(Resource):
    '''
    修改头像图片数据视图
    '''
    method_decorators = [login_required]

    def patch(self):
        # 接收前端传过来的参数 user_id , 图片数据
        user_id = g.user_id

        # 校验图片数据
        parser = RequestParser()
        parser.add_argument('photo', required=True, type=image_file, location='files')
        result = parser.parse_args()

        # 接收图片二进制数据
        photo_data = result.get('photo').read()

        # 使用七牛云上传工具
        pic_name = pic_upload(photo_data)

        # 判断是否上传成功
        if pic_name is None:
            return {'message': 'Upload picture failed!.'}, 404

        # 查询此用户是否存在
        try:
            # 存在，修改user.profile_photo的值
            user = User.query.get(user_id)
            user.profile_photo = pic_name
            db.session.commit()
        except Exception as e:
            # 不存在，数据库回滚
            current_app.logger.error(e)
            db.session.rollback()
            return {'message': 'Database error！'}, 507

        full_path = current_app.config['QINIU_DOMAIN'] + pic_name

        return {'full_path': full_path}


class CurrentUserResource(Resource):
    '''通过自己封装的缓存工具类来查询用户数据'''
    method_decorators = [login_required]

    def get(self):
        '''获取用户数据'''
        user_id = g.user_id

        try:

            user = UserProfileCache(user_id).get()
        except DatabaseError as e:
            current_app.logger.error(e)
            return {"message": "DatabaseError"}

        user['id'] = user_id

        # 增加用户发布文章数量并获取响应给前端
        UserArticleCountStorage.incr(user_id, 99)
        user['article_count'] = UserArticleCountStorage.get(user_id)

        return user
