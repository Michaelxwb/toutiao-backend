import json
import time

from flask import current_app, g
from redis import StrictRedis
from redis.exceptions import RedisError
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import load_only

from cache import persstorage as cache_statistic
from models.news import Article, Collection, Attitude, CommentLiking
from models.user import User, Relation, UserProfile
from . import constants


class UserProfileCache(object):
    '''
    缓存用户基本信息工具类
    '''

    def __init__(self, user_id):
        self.key = 'user:{}:profile'.format(user_id)
        self.user_id = user_id

    def save(self):
        # 1、查询缓存redis缓存
        redis_cluster = current_app.redis_cluster

        try:
            user = User.query.options(load_only(
                User.name,
                User.mobile,
                User.profile_photo,
                User.certificate,
                User.introduction
            )).filter_by(id=self.user_id).first()
        except DatabaseError as e:
            current_app.logger.error(e)
            raise e

        if user:
            user_dict = {
                'name': user.name,
                'mobile': user.mobile,
                'profile_photo': user.profile_photo,
                'certificate': user.certificate,
                'introduction': user.introduction
            }
            user_str = json.dumps(user_dict)

            redis_cluster.setex(self.key, constants.UserProfileCacheTTL.get_time(), user_str)
            # 4、mysql有值，返回并回填
            return user_dict

        else:
            # 5、无值，回填-1标识
            redis_cluster.setex(self.key, constants.UserNotExistsCacheTTL.get_time(), '-1')
            return None

    def get(self):
        '''缓存工具类'''
        # 1、查询缓存redis缓存
        redis_cluster = current_app.redis_cluster
        # 2、有值返回，
        try:
            cache_data = redis_cluster.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            cache_data = None

        if cache_data:
            if cache_data == b'-1':
                return None
            else:
                return json.loads(cache_data)
        # 3、无值，查询mysql
        else:
            return self.save()

    def clear(self):
        '''清除缓存'''
        # 1、查询缓存redis缓存
        redis_cluster = current_app.redis_cluster  # type: StrictRedis
        try:
            redis_cluster.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            raise e

    def exist(self):
        '''判断缓存是否存在'''
        # 如果缓存存在则返回True，否则返回False
        # 1、查询缓存redis缓存
        redis_cluster = current_app.redis_cluster
        # 2、有值返回，
        try:
            cache_data = redis_cluster.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            cache_data = None

        if cache_data:
            if cache_data == b'-1':
                return False
            else:
                return True
        else:
            result = self.save()
            if result:
                return True
            else:
                return False


# ===============================新增加===================================

class UserStatusCache(object):
    """
    用户状态缓存
    """

    def __init__(self, user_id):
        self.key = 'user:{}:status'.format(user_id)
        self.user_id = user_id

    def save(self, status):
        """
        设置用户状态缓存
        :param status:
        """

        try:
            current_app.redis_cluster.setex(self.key, constants.UserStatusCacheTTL.get_val(), status)
        except RedisError as e:
            current_app.logger.error(e)

    def get(self):
        """
        获取用户状态
        :return:
        """
        rc = current_app.redis_cluster

        try:
            status = rc.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            status = None

        if status is not None:
            return status
        else:
            user = User.query.options(load_only(User.status)).filter_by(id=self.user_id).first()
            if user:
                self.save(user.status)
                return user.status
            else:
                return False


class UserAdditionalProfileCache(object):
    """
    用户附加资料缓存（如性别、生日等）
    """

    def __init__(self, user_id):
        self.key = 'user:{}:profilex'.format(user_id)
        self.user_id = user_id

    def get(self):
        """
        获取用户的附加资料（如性别、生日等）
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.get(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            return json.loads(ret)
        else:
            profile = UserProfile.query.options(load_only(UserProfile.gender, UserProfile.birthday)) \
                .filter_by(id=self.user_id).first()
            profile_dict = {
                'gender': profile.gender,
                'birthday': profile.birthday.strftime('%Y-%m-%d') if profile.birthday else ''
            }
            try:
                rc.setex(self.key, constants.UserAdditionalProfileCacheTTL.get_val(), json.dumps(profile_dict))
            except RedisError as e:
                current_app.logger.error(e)
            return profile_dict

    def clear(self):
        """
        清除用户的附加资料
        :return:
        """
        try:
            current_app.redis_cluster.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)


class UserFollowingCache(object):
    """
    用户关注缓存数据
    """

    def __init__(self, user_id):
        self.key = 'user:{}:following'.format(user_id)
        self.user_id = user_id

    def get(self):
        """
        获取用户的关注列表
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.zrevrange(self.key, 0, -1)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            # In order to be consistent with db data type.
            return [int(uid) for uid in ret]

        # 为了防止缓存击穿，先尝试从缓存中判断关注数是否为0，若为0不再查询数据库
        ret = cache_statistic.UserFollowingsCountStorage.get(self.user_id)
        if ret == 0:
            return []

        ret = Relation.query.options(load_only(Relation.target_user_id, Relation.utime)) \
            .filter_by(user_id=self.user_id, relation=Relation.RELATION.FOLLOW) \
            .order_by(Relation.utime.desc()).all()

        followings = []
        cache = []
        for relation in ret:
            followings.append(relation.target_user_id)
            cache.append(relation.utime.timestamp())
            cache.append(relation.target_user_id)

        if cache:
            try:
                pl = rc.pipeline()
                pl.zadd(self.key, *cache)
                pl.expire(self.key, constants.UserFollowingsCacheTTL.get_val())
                results = pl.execute()
                if results[0] and not results[1]:
                    rc.delete(self.key)
            except RedisError as e:
                current_app.logger.error(e)

        return followings

    def determine_follows_target(self, target_user_id):
        """
        判断用户是否关注了目标用户
        :param target_user_id: 被关注的用户id
        :return:
        """
        followings = self.get()

        return int(target_user_id) in followings

    def update(self, target_user_id, timestamp, increment=1):
        """
        更新用户的关注缓存数据
        :param target_user_id: 被关注的目标用户
        :param timestamp: 关注时间戳
        :param increment: 增量
        :return:
        """
        rc = current_app.redis_cluster

        # Update user following user id list
        try:
            ttl = rc.ttl(self.key)
            if ttl > constants.ALLOW_UPDATE_FOLLOW_CACHE_TTL_LIMIT:
                if increment > 0:
                    rc.zadd(self.key, timestamp, target_user_id)
                else:
                    rc.zrem(self.key, target_user_id)
        except RedisError as e:
            current_app.logger.error(e)


class UserRelationshipCache(object):
    """
    用户关系缓存数据
    """

    def __init__(self, user_id):
        self.key = 'user:{}:relationship'.format(user_id)
        self.user_id = user_id

    def get(self):
        """
        获取用户的关系数据
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.hgetall(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            # 为了防止缓存击穿
            if b'-1' in ret:
                return {}
            else:
                # In order to be consistent with db data type.
                return {int(uid): int(relation) for uid, relation in ret.items()}

        ret = Relation.query.options(load_only(Relation.target_user_id, Relation.utime, Relation.relation)) \
            .filter(Relation.user_id == self.user_id, Relation.relation != Relation.RELATION.DELETE) \
            .order_by(Relation.utime.desc()).all()

        relations = {}
        for relation in ret:
            relations[relation.target_user_id] = relation.relation

        pl = rc.pipeline()
        try:
            if relations:
                pl.hmset(self.key, relations)
                pl.expire(self.key, constants.UserFollowingsCacheTTL.get_val())
            else:
                pl.hmset(self.key, {-1: -1})
                pl.expire(self.key, constants.UserRelationshipNotExistsCacheTTL.get_val())
            results = pl.execute()
            if results[0] and not results[1]:
                rc.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)

        return relations

    def determine_follows_target(self, target_user_id):
        """
        判断用户是否关注了目标用户
        :param target_user_id: 被关注的用户id
        :return:
        """
        relations = self.get()

        return relations.get(target_user_id) == Relation.RELATION.FOLLOW

    def determine_blacklist_target(self, target_user_id):
        """
        判断是否已拉黑目标用户
        :param target_user_id:
        :return:
        """
        relations = self.get()

        return relations.get(target_user_id) == Relation.RELATION.BLACKLIST

    def clear(self):
        """
        清除
        """
        current_app.redis_cluster.delete(self.key)


class UserFollowersCache(object):
    """
    用户粉丝缓存
    """

    def __init__(self, user_id):
        self.key = 'user:{}:fans'.format(user_id)
        self.user_id = user_id

    def get(self):
        """
        获取用户的粉丝列表
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.zrevrange(self.key, 0, -1)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            # In order to be consistent with db data type.
            return [int(uid) for uid in ret]

        ret = cache_statistic.UserFollowersCountStorage.get(self.user_id)
        if ret == 0:
            return []

        ret = Relation.query.options(load_only(Relation.user_id, Relation.utime)) \
            .filter_by(target_user_id=self.user_id, relation=Relation.RELATION.FOLLOW) \
            .order_by(Relation.utime.desc()).all()

        followers = []
        cache = []
        for relation in ret:
            followers.append(relation.user_id)
            cache.append(relation.utime.timestamp())
            cache.append(relation.user_id)

        if cache:
            try:
                pl = rc.pipeline()
                pl.zadd(self.key, *cache)
                pl.expire(self.key, constants.UserFansCacheTTL.get_val())
                results = pl.execute()
                if results[0] and not results[1]:
                    rc.delete(self.key)
            except RedisError as e:
                current_app.logger.error(e)

        return followers

    def update(self, target_user_id, timestamp, increment=1):
        """
        更新粉丝数缓存
        """
        rc = current_app.redis_cluster
        try:
            ttl = rc.ttl(self.key)
            if ttl > constants.ALLOW_UPDATE_FOLLOW_CACHE_TTL_LIMIT:
                if increment > 0:
                    rc.zadd(self.key, timestamp, target_user_id)
                else:
                    rc.zrem(self.key, target_user_id)
        except RedisError as e:
            current_app.logger.error(e)


class UserReadingHistoryStorage(object):
    """
    用户阅读历史
    """

    def __init__(self, user_id):
        self.key = 'user:{}:his:reading'.format(user_id)
        self.user_id = user_id

    def save(self, article_id):
        """
        保存用户阅读历史
        :param article_id: 文章id
        :return:
        """
        try:
            pl = current_app.redis_master.pipeline()
            pl.zadd(self.key, time.time(), article_id)
            pl.zremrangebyrank(self.key, 0, -1 * (constants.READING_HISTORY_COUNT_PER_USER + 1))
            pl.execute()
        except RedisError as e:
            current_app.logger.error(e)

    def get(self, page, per_page):
        """
        获取阅读历史
        """
        r = current_app.redis_master
        try:
            total_count = r.zcard(self.key)
        except ConnectionError as e:
            r = current_app.redis_slave
            total_count = r.zcard(self.key)

        article_ids = []
        if total_count > 0 and (page - 1) * per_page < total_count:
            try:
                article_ids = r.zrevrange(self.key, (page - 1) * per_page, page * per_page - 1)
            except ConnectionError as e:
                current_app.logger.error(e)
                article_ids = current_app.redis_slave.zrevrange(self.key, (page - 1) * per_page, page * per_page - 1)

        return total_count, article_ids


class UserSearchingHistoryStorage(object):
    """
    用户搜索历史
    """

    def __init__(self, user_id):
        self.key = 'user:{}:his:searching'.format(user_id)
        self.user_id = user_id

    def save(self, keyword):
        """
        保存用户搜索历史
        :param keyword: 关键词
        :return:
        """
        pl = current_app.redis_master.pipeline()
        pl.zadd(self.key, time.time(), keyword)
        pl.zremrangebyrank(self.key, 0, -1 * (constants.SEARCHING_HISTORY_COUNT_PER_USER + 1))
        pl.execute()

    def get(self):
        """
        获取搜索历史
        """
        try:
            keywords = current_app.redis_master.zrevrange(self.key, 0, -1)
        except ConnectionError as e:
            current_app.logger.error(e)
            keywords = current_app.redis_slave.zrevrange(self.key, 0, -1)

        keywords = [keyword.decode() for keyword in keywords]
        return keywords

    def clear(self):
        """
        清除
        """
        current_app.redis_master.delete(self.key)


class UserArticlesCache(object):
    """
    用户文章缓存
    """

    def __init__(self, user_id):
        self.user_id = user_id
        self.key = 'user:{}:art'.format(user_id)

    def get_page(self, page, per_page):
        """
        获取用户的文章列表
        :param page: 页数
        :param per_page: 每页数量
        :return: total_count, [article_id, ..]
        """
        rc = current_app.redis_cluster

        try:
            pl = rc.pipeline()
            pl.zcard(self.key)
            pl.zrevrange(self.key, (page - 1) * per_page, page * per_page)
            total_count, ret = pl.execute()
        except RedisError as e:
            current_app.logger.error(e)
            total_count = 0
            ret = []

        if total_count > 0:
            # Cache exists.
            return total_count, [int(aid) for aid in ret]
        else:
            # No cache.
            total_count = cache_statistic.UserArticleCountStorage.get(self.user_id)
            if total_count == 0:
                return 0, []

            ret = Article.query.options(load_only(Article.id, Article.ctime)) \
                .filter_by(user_id=self.user_id, status=Article.STATUS.APPROVED) \
                .order_by(Article.ctime.desc()).all()

            articles = []
            cache = []
            for article in ret:
                articles.append(article.id)
                cache.append(article.ctime.timestamp())
                cache.append(article.id)

            if cache:
                try:
                    pl = rc.pipeline()
                    pl.zadd(self.key, *cache)
                    pl.expire(self.key, constants.UserArticlesCacheTTL.get_val())
                    results = pl.execute()
                    if results[0] and not results[1]:
                        rc.delete(self.key)
                except RedisError as e:
                    current_app.logger.error(e)

            total_count = len(articles)
            page_articles = articles[(page - 1) * per_page:page * per_page]

            return total_count, page_articles

    def clear(self):
        """
        清除
        """
        rc = current_app.redis_cluster
        rc.delete(self.key)


class UserArticleCollectionsCache(object):
    """
    用户收藏文章缓存
    """

    def __init__(self, user_id):
        self.user_id = user_id
        self.key = 'user:{}:art:collection'.format(user_id)

    def get_page(self, page, per_page):
        """
        获取用户的文章列表
        :param page: 页数
        :param per_page: 每页数量
        :return: total_count, [article_id, ..]
        """
        rc = current_app.redis_cluster

        try:
            pl = rc.pipeline()
            pl.zcard(self.key)
            pl.zrevrange(self.key, (page - 1) * per_page, page * per_page)
            total_count, ret = pl.execute()
        except RedisError as e:
            current_app.logger.error(e)
            total_count = 0
            ret = []

        if total_count > 0:
            # Cache exists.
            return total_count, [int(aid) for aid in ret]
        else:
            # No cache.
            total_count = cache_statistic.UserArticleCollectingCountStorage.get(self.user_id)
            if total_count == 0:
                return 0, []

            ret = Collection.query.options(load_only(Collection.article_id, Collection.utime)) \
                .filter_by(user_id=self.user_id, is_deleted=False) \
                .order_by(Collection.utime.desc()).all()

            collections = []
            cache = []
            for collection in ret:
                collections.append(collection.article_id)
                cache.append(collection.utime.timestamp())
                cache.append(collection.article_id)

            if cache:
                try:
                    pl = rc.pipeline()
                    pl.zadd(self.key, *cache)
                    pl.expire(self.key, constants.UserArticleCollectionsCacheTTL.get_val())
                    results = pl.execute()
                    if results[0] and not results[1]:
                        rc.delete(self.key)
                except RedisError as e:
                    current_app.logger.error(e)

            total_count = len(collections)
            page_articles = collections[(page - 1) * per_page:page * per_page]

            return total_count, page_articles

    def clear(self):
        """
        清除
        """
        current_app.redis_cluster.delete(self.key)

    def determine_collect_target(self, target):
        """
        判断用户是否收藏了指定文章
        :param target:
        :return:
        """
        total_count, collections = self.get_page(1, -1)
        return target in collections


class UserArticleAttitudeCache(object):
    """
    用户文章态度缓存数据
    """

    def __init__(self, user_id):
        self.key = 'user:{}:art:attitude'.format(user_id)
        self.user_id = user_id

    def get_all(self):
        """
        获取用户文章态度数据
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.hgetall(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            # 为了防止缓存击穿
            if b'-1' in ret:
                return {}
            else:
                # In order to be consistent with db data type.
                return {int(aid): int(attitude) for aid, attitude in ret.items()}

        ret = Attitude.query.options(load_only(Attitude.article_id, Attitude.attitude)) \
            .filter(Attitude.user_id == self.user_id, Attitude.attitude != None).all()

        attitudes = {}
        for atti in ret:
            attitudes[atti.article_id] = atti.attitude

        pl = rc.pipeline()
        try:
            if attitudes:
                pl.hmset(self.key, attitudes)
                pl.expire(self.key, constants.UserArticleAttitudeCacheTTL.get_val())
            else:
                pl.hmset(self.key, {-1: -1})
                pl.expire(self.key, constants.UserArticleAttitudeNotExistsCacheTTL.get_val())
            results = pl.execute()
            if results[0] and not results[1]:
                rc.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)

        return attitudes

    def get_article_attitude(self, article_id):
        """
        获取指定文章态度
        :param article_id:
        :return:
        """
        if hasattr(g, 'article_attitudes'):
            attitudes = g.article_attitudes
        else:
            attitudes = self.get_all()
            g.article_attitudes = attitudes

        return attitudes.get(article_id, -1)

    def determine_liking_article(self, article_id):
        """
        判断是否对文章点赞
        :param article_id:
        :return:
        """
        return self.get_article_attitude(article_id) == Attitude.ATTITUDE.LIKING

    def clear(self):
        """
        清除
        """
        current_app.redis_cluster.delete(self.key)


class UserCommentLikingCache(object):
    """
    用户评论点赞缓存数据
    """

    def __init__(self, user_id):
        self.key = 'user:{}:comm:liking'.format(user_id)
        self.user_id = user_id

    def get(self):
        """
        获取用户文章评论点赞数据
        :return:
        """
        rc = current_app.redis_cluster

        try:
            ret = rc.smembers(self.key)
        except RedisError as e:
            current_app.logger.error(e)
            ret = None

        if ret:
            # 为了防止缓存击穿
            if b'-1' in ret:
                return []
            else:
                # In order to be consistent with db data type.
                return set([int(cid) for cid in ret])

        ret = CommentLiking.query.options(load_only(CommentLiking.comment_id)) \
            .filter(CommentLiking.user_id == self.user_id, CommentLiking.is_deleted == False).all()

        cids = [com.comment_id for com in ret]
        pl = rc.pipeline()
        try:
            if cids:
                pl.sadd(self.key, *cids)
                pl.expire(self.key, constants.UserCommentLikingCacheTTL.get_val())
            else:
                pl.sadd(self.key, -1)
                pl.expire(self.key, constants.UserCommentLikingNotExistsCacheTTL.get_val())
            results = pl.execute()
            if results[0] and not results[1]:
                rc.delete(self.key)
        except RedisError as e:
            current_app.logger.error(e)

        return set(cids)

    def determine_liking_comment(self, comment_id):
        """
        判断是否对文章点赞
        :param comment_id:
        :return:
        """
        if hasattr(g, self.key):
            liking_comments = getattr(g, self.key)
        else:
            liking_comments = self.get()
            setattr(g, self.key, liking_comments)

        return comment_id in liking_comments

    def clear(self):
        """
        清除
        """
        current_app.redis_cluster.delete(self.key)


def get_user_articles(user_id):
    """
    获取用户的所有文章列表 已废弃
    :param user_id:
    :return:
    """
    r = current_app.redis_cli['user_cache']
    timestamp = time.time()

    ret = r.zrevrange('user:{}:art'.format(user_id), 0, -1)
    if ret:
        r.zadd('user:art', timestamp, user_id)
        return [int(aid) for aid in ret]

    ret = r.hget('user:{}'.format(user_id), 'art_count')
    if ret is not None and int(ret) == 0:
        return []

    ret = Article.query.options(load_only(Article.id, Article.ctime)) \
        .filter_by(user_id=user_id, status=Article.STATUS.APPROVED) \
        .order_by(Article.ctime.desc()).all()

    articles = []
    cache = []
    for article in ret:
        articles.append(article.id)
        cache.append(article.ctime.timestamp())
        cache.append(article.id)

    if cache:
        pl = r.pipeline()
        pl.zadd('user:art', timestamp, user_id)
        pl.zadd('user:{}:art'.format(user_id), *cache)
        pl.execute()

    return articles
