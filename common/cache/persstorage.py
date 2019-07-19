from flask import current_app
from redis import StrictRedis
from redis.exceptions import RedisError
from sqlalchemy import func
from sqlalchemy.orm import load_only

from models import db
from models.news import Article, Collection, Attitude, CommentLiking, Comment, ArticleStatistic
from models.user import Relation


class BaseCountStorage(object):
    key = ''

    @classmethod
    def get(cls, user_id):
        # 获取主从连接对象

        article_count = None
        redis_master = current_app.redis_master  # type: StrictRedis
        redis_slave = current_app.redis_slave  # type: StrictRedis

        try:
            article_count = redis_master.zscore(cls.key, user_id)
        except RedisError as e:
            current_app.logger.error(e)
            article_count = redis_slave.zscore(cls.key, user_id)

        if article_count:
            return int(article_count)
        else:
            return 0

    @classmethod
    def incr(cls, user_id, incr_num=1):
        redis_master = current_app.redis_master  # type: StrictRedis
        try:
            redis_master.zincrby(cls.key, user_id, incr_num)
        except RedisError as e:
            current_app.logger.error(e)
            raise e

    # 将最新的修正数据保存到redis中
    @classmethod
    def reset(cls, result):
        redis_master = current_app.redis_master  # type: StrictRedis

        pipeline = redis_master.pipeline()
        pipeline.delete(cls.key)
        for user_id, count in result:
            pipeline.zadd(cls.key, count, user_id)
        pipeline.execute()


class UserArticleCountStorage(BaseCountStorage):
    """用户发布的文章数量统计"""
    key = "count:user:articles"

    @staticmethod
    def db_query():
        result = db.session.query(Article.user_id, func.count(Article.id)) \
            .filter(Article.status == Article.STATUS.APPROVED) \
            .group_by(Article.user_id).all()

        return result


class UserArticleLikingCountStorage(BaseCountStorage):
    """用户文章点赞数量统计"""
    key = "count:user:liking"

    @staticmethod
    def db_query():
        ret = db.session.query(Attitude.article_id, func.count(Collection.article_id)) \
            .filter(Attitude.attitude == Attitude.ATTITUDE.LIKING).group_by(Collection.article_id).all()
        return ret


# 使用：UserArticleCountStorage.get(1)

class UserFollowingsCountStorage(BaseCountStorage):
    """
    用户关注数量
    """
    key = 'count:user:followings'

    @staticmethod
    def db_query():
        return db.session.query(Relation.user_id, func.count(Relation.target_user_id)) \
            .filter(Relation.relation == Relation.RELATION.FOLLOW) \
            .group_by(Relation.user_id).all()


class UserArticleCollectingCountStorage(BaseCountStorage):
    """
    用户收藏数量
    """
    key = 'count:user:art:collecting'

    @staticmethod
    def db_query():
        ret = db.session.query(Collection.user_id, func.count(Collection.article_id)) \
            .filter(Collection.is_deleted == 0).group_by(Collection.user_id).all()
        return ret


class ArticleDislikeCountStorage(BaseCountStorage):
    """
    文章不喜欢数据
    """
    key = 'count:art:dislike'

    @staticmethod
    def db_query():
        ret = db.session.query(Attitude.article_id, func.count(Collection.article_id)) \
            .filter(Attitude.attitude == Attitude.ATTITUDE.DISLIKE).group_by(Collection.article_id).all()
        return ret


class CommentLikingCountStorage(BaseCountStorage):
    """
    评论点赞数据
    """
    key = 'count:comm:liking'

    @staticmethod
    def db_query():
        ret = db.session.query(CommentLiking.comment_id, func.count(CommentLiking.comment_id)) \
            .filter(CommentLiking.is_deleted == 0).group_by(CommentLiking.comment_id).all()
        return ret


class ArticleCommentCountStorage(BaseCountStorage):
    """
    文章评论数量
    """
    key = 'count:art:comm'

    @staticmethod
    def db_query():
        ret = db.session.query(Comment.article_id, func.count(Comment.id)) \
            .filter(Comment.status == Comment.STATUS.APPROVED).group_by(Comment.article_id).all()
        return ret


class CommentReplyCountStorage(BaseCountStorage):
    """
    评论回复数量
    """
    key = 'count:art:reply'

    @staticmethod
    def db_query():
        ret = db.session.query(Comment.parent_id, func.count(Comment.id)) \
            .filter(Comment.status == Comment.STATUS.APPROVED, Comment.parent_id != None) \
            .group_by(Comment.parent_id).all()
        return ret


class UserFollowersCountStorage(BaseCountStorage):
    """
    用户粉丝数量
    """
    key = 'count:user:followers'

    @staticmethod
    def db_query():
        ret = db.session.query(Relation.target_user_id, func.count(Relation.user_id)) \
            .filter(Relation.relation == Relation.RELATION.FOLLOW) \
            .group_by(Relation.target_user_id).all()
        return ret


class UserLikedCountStorage(BaseCountStorage):
    """
    用户被赞数量
    """
    key = 'count:user:liked'

    @staticmethod
    def db_query():
        ret = db.session.query(Article.user_id, func.count(Attitude.id)).join(Attitude.article) \
            .filter(Attitude.attitude == Attitude.ATTITUDE.LIKING) \
            .group_by(Article.user_id).all()
        return ret


class ArticleReadingCountStorage(BaseCountStorage):
    """
    文章阅读量
    """
    key = 'count:art:reading'

    # @staticmethod
    # def db_query():
    #     ret = Article.query.filter_by(status=Article.STATUS.APPROVED).all()
    #     return ret


class UserArticlesReadingCountStorage(BaseCountStorage):
    """
    作者的文章阅读总量
    """
    kye = 'count:user:arts:reading'

    # @staticmethod
    # def db_query():
    #     ret = Article.query(Article.user_id, func.count(Attitude.id)).join(Attitude.article) \
    #         .filter(Attitude.attitude == Attitude.ATTITUDE.LIKING) \
    #         .group_by(Article.user_id).all()
    #     return ret
