import json

from flask import current_app
from flask_restful import marshal, fields
from redis import StrictRedis
from redis.exceptions import RedisError
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import load_only

from cache import constants
from cache import user as cache_user
from models.news import Article, Attitude
from models.user import User


class ArticleInfoCache(object):
    '''
    文章信息缓存表
    'title': fields.String(attribute='title'),
    'aut_id': fields.Integer(attribute='user_id'),
    'pubdate': fields.DateTime(attribute='ctime', dt_format='iso8601'),
    'ch_id': fields.Integer(attribute='channel_id'),
    'allow_comm': fields.Integer(attribute='allow_comment'),
    '''

    def __init__(self, article_id):
        self.key = 'art:{}:info'.format(article_id)
        self.article_id = article_id

    def save(self):
        # 获取redis集群对象
        redis_cluster = current_app.redis_cluster

        # redis中没有缓存的数据，查询mysql
        try:
            article_info = Article.query.options(load_only(
                Article.title,
                Article.user_id,
                Article.ctime,
                Article.channel_id,
                Article.allow_comment,
                Article.comment_count
            )).filter_by(id=self.article_id, status=Article.STATUS.APPROVED).first()
        except DatabaseError as e:
            current_app.logger.error(e)
            raise e

        # 有值返回并回填
        if article_info:
            # 查询出对应的作者的相关信息
            try:
                auth_info = User.query.options(load_only(
                    User.id,
                    User.name,
                    User.like_count,
                    User.article_count
                )).filter_by(id=article_info.user_id).first()
            except DatabaseError as e:
                current_app.logger.error(e)
                raise e

            article_dict = {
                'id': self.article_id,
                'title': article_info.title,
                'auth': auth_info.id,
                'pubdate': article_info.ctime.strftime('%Y-%m-%d %H:%M:%S'),
                'channel_id': article_info.channel_id,
                'comment_count': article_info.comment_count,
                'allow_comment': article_info.allow_comment,
                'like_count': auth_info.like_count,
                'article_count': auth_info.article_count
            }

            article_info_str = json.dumps(article_dict)

            redis_cluster.setex(self.key, constants.ArticleInfoCacheTTL.get_time(), article_info_str)

            # 4、mysql有值，返回并回填
            return article_dict

        else:
            # 5、无值，回填-1标识
            redis_cluster.setex(self.key, constants.ArticleNotExistsCacheTTL.get_time(), '-1')
            return None

    def get(self):
        # 获取redis集群对象
        redis_cluster = current_app.redis_cluster

        # 从redis中获取文章缓存数据
        try:
            article_cache = redis_cluster.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            article_cache = None

        # redis中有缓存的数据
        if article_cache:
            # 防止缓存穿透存的-1标识
            if article_cache == b'-1':
                return None
            else:
                return json.loads(article_cache)
        else:

            return self.save()

    def exist(self):
        '''
        判断此文章是否存在缓存
        :return:
        '''
        # 获取redis集群对象
        redis_cluster = current_app.redis_cluster

        # 从redis中获取文章缓存数据
        try:
            article_cache = redis_cluster.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            article_cache = None

        # redis中有缓存的数据
        if article_cache is not None:
            return False if article_cache == b'-1' else True

        else:

            result = self.save()

            if result is None:
                return False
            else:
                return True

    def determine_allow_comment(self):
        """
        判断是否允许评论
        """
        redis_cluster = current_app.redis_cluster
        try:
            article_cache = redis_cluster.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            article_cache = None

        if article_cache is None:
            article_formatted = self.save()
        else:
            article_formatted = json.loads(article_cache)

        return article_formatted['allow_comment']

    def clear(self):
        '''清空缓存'''
        redis_cluster = current_app.redis_cluster  # type: StrictRedis
        redis_cluster.delete(self.key)


class ChannelTopArticlesStorage(object):
    """
    频道置顶文章缓存
    使用redis持久保存
    """

    def __init__(self, channel_id):
        self.key = 'ch:{}:art:top'.format(channel_id)
        self.channel_id = channel_id

    def get(self):
        """
        获取指定频道的置顶文章id
        :return: [article_id, ...]
        """
        try:
            ret = current_app.redis_master.zrevrange(self.key, 0, -1)
        except ConnectionError as e:
            current_app.logger.error(e)
            ret = current_app.redis_slave.zrevrange(self.key, 0, -1)

        if not ret:
            return []
        else:
            return [int(article_id) for article_id in ret]

    def exists(self, article_id):
        """
        判断文章是否置顶
        :param article_id:
        :return:
        """
        try:
            rank = current_app.redis_master.zrank(self.key, article_id)
        except ConnectionError as e:
            current_app.logger.error(e)
            rank = current_app.redis_slave.zrank(self.key, article_id)

        return 0 if rank is None else 1


class ArticleDetailCache(object):
    """
    文章详细内容缓存
    """
    article_fields = {
        'art_id': fields.Integer(attribute='id'),
        'title': fields.String(attribute='title'),
        'pubdate': fields.DateTime(attribute='ctime', dt_format='iso8601'),
        'content': fields.String(attribute='content.content'),
        'aut_id': fields.Integer(attribute='user_id'),
        'ch_id': fields.Integer(attribute='channel_id'),
    }

    def __init__(self, article_id):
        self.key = 'art:{}:detail'.format(article_id)
        self.article_id = article_id

    def get(self):
        """
        获取文章详情信息
        :return:
        """
        # 查询文章数据
        rc = current_app.redis_cluster
        try:
            article_bytes = rc.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            article_bytes = None

        if article_bytes:
            # 使用缓存
            article_dict = json.loads(article_bytes)
        else:
            # 查询数据库
            article = Article.query.options(load_only(
                Article.id,
                Article.user_id,
                Article.title,
                Article.is_advertising,
                Article.ctime,
                Article.channel_id
            )).filter_by(id=self.article_id, status=Article.STATUS.APPROVED).first()

            article_dict = marshal(article, self.article_fields)

            # 缓存
            article_cache = json.dumps(article_dict)
            try:
                rc.setex(self.key, constants.ArticleDetailCacheTTL.get_time(), article_cache)
            except RedisError:
                pass

        user = cache_user.UserProfileCache(article_dict['aut_id']).get()

        article_dict['aut_name'] = user['name']
        article_dict['aut_photo'] = user['photo']

        return article_dict

    def clear(self):
        current_app.redis_cluster.delete(self.key)


class ArticleUserAttitudeCache(object):
    """
    用户对文章态度的缓存，点赞或不喜欢
    """

    def __init__(self, user_id, article_id):
        self.user_id = user_id
        self.article_id = article_id
        self.key = 'user:{}:art:{}:liking'.format(user_id, article_id)

    def get(self):
        """
        获取
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret is not None:
            ret = int(ret)
            return ret

        att = Attitude.query.options(load_only(Attitude.attitude)) \
            .filter_by(user_id=self.user_id, article_id=self.article_id).first()
        ret = att.attitude if att and att.attitude else -1

        try:
            rc.setex(self.key, constants.ArticleUserNoAttitudeCacheTTL.get_time(), int(ret))
        except RedisError as e:
            current_app.logger.error(e)

        return ret

    def clear(self):
        """
        清除
        :return:
        """
        rc = current_app.redis_cluster
        try:
            rc.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)
