import random


# 缓存评论最大SCORE
COMMENTS_CACHE_MAX_SCORE = 2e19

# 默认用户头像
DEFAULT_USER_PROFILE_PHOTO = 'Fkj6tQi3xJwVXi1u2swCElotfdCi'  # 程序猿

# 阅读历史每人保存数目
READING_HISTORY_COUNT_PER_USER = 100

# 用户搜索历史每人保存数目
SEARCHING_HISTORY_COUNT_PER_USER = 4

# 允许用户资料数据缓存更新的TTL限制，秒
# ALLOW_UPDATE_USER_PROFILE_CACHE_TTL_LIMIT = 5

# 允许用户资料数据缓存中统计数据更新的TTL限制，秒
# ALLOW_UPDATE_USER_PROFILE_STATISTIC_CACHE_TTL_LIMIT = 5 + ALLOW_UPDATE_USER_PROFILE_CACHE_TTL_LIMIT

# 允许更新关注缓存的TTL限制，秒
ALLOW_UPDATE_FOLLOW_CACHE_TTL_LIMIT = 5

# 默认用户频道缓存有效期，秒
DEFAULT_USER_CHANNELS_CACHE_TTL = 24 * 60 * 60

# 全部频道缓存有效期，秒
ALL_CHANNELS_CACHE_TTL = 24 * 60 * 60

# 允许更新文章评论列表缓存的TTL限制，秒
ALLOW_UPDATE_ARTICLE_COMMENTS_CACHE_TTL_LIMIT = 5

# 系统公告缓存时间，秒
ANNOUNCEMENTS_CACHE_TTL = 48 * 60 * 60


class BaseCacheTTL(object):
    """
    防止缓存雪崩，设置随机有效时长  基准值 + 随机值  单位s
    """

    # 基准值
    TTL = 30 * 60
    # 随机值的上线
    MAX_DELTA = 10 * 60

    @classmethod
    def get_time(cls):
        # 随机过期时长
        return cls.TTL + random.randrange(0, cls.MAX_DELTA)


class UserProfileCacheTTL(BaseCacheTTL):
    """
    用户数据设置有效时长
    """
    TTL = 60 * 60
    MAX_DELTA = 16 * 60


class UserStatusCacheTTL(BaseCacheTTL):
    """
    用户数据设置有效时长
    """
    TTL = 60 * 60
    MAX_DELTA = 16 * 60


class UserNotExistsCacheTTL(BaseCacheTTL):
    """
    用户数据不存在的设置有效时长，因为大量并发请求只会在短时间内攻击
    """
    TTL = 10 * 60
    MAX_DELTA = 5 * 60


# BaseCacheTTL.get_value()

# ===============================新增加===================================

class UserFollowingsCacheTTL(BaseCacheTTL):
    """
    用户关注列表缓存时间，秒
    """
    TTL = 30 * 60


class UserRelationshipCacheTTL(BaseCacheTTL):
    """
    用户关系缓存时间，秒
    """
    TTL = 30 * 60


class UserRelationshipNotExistsCacheTTL(BaseCacheTTL):
    """
    用户关系不存在数据缓存时间，秒
    """
    TTL = 5 * 60
    MAX_DELTA = 60


class UserAdditionalProfileCacheTTL(BaseCacheTTL):
    """
    用户详细资料缓存时间，秒
    """
    TTL = 10 * 60
    MAX_DELTA = 2 * 60


class UserFansCacheTTL(BaseCacheTTL):
    """
    用户粉丝列表缓存时间，秒
    """
    TTL = 30 * 60


class UserChannelsCacheTTL(BaseCacheTTL):
    """
    用户频道缓存时间，秒
    """
    TTL = 60 * 60


class UserArticleAttitudeCacheTTL(BaseCacheTTL):
    """
    用户文章态度缓存时间，秒
    """
    TTL = 30 * 60


class UserArticleAttitudeNotExistsCacheTTL(BaseCacheTTL):
    """
    用户文章态度不存在数据缓存时间，秒
    """
    TTL = 5 * 60
    MAX_DELTA = 60


class UserCommentLikingCacheTTL(BaseCacheTTL):
    """
    用户文章评论点赞缓存时间，秒
    """
    TTL = 10 * 60
    MAX_DELTA = 2 * 60


class UserCommentLikingNotExistsCacheTTL(BaseCacheTTL):
    """
    用户文章评论点赞不存在数据缓存时间，秒
    """
    TTL = 3 * 60
    MAX_DELTA = 60


class ArticleInfoCacheTTL(BaseCacheTTL):
    """
    文章信息缓存时间，秒
    """
    TTL = 30 * 60


class ArticleNotExistsCacheTTL(BaseCacheTTL):
    """
    文章不存在结果缓存
    为解决缓存击穿，有效期不宜过长
    """
    TTL = 5 * 60
    MAX_DELTA = 60


class ArticleDetailCacheTTL(BaseCacheTTL):
    """
    文章详细内容缓存时间，秒
    """
    TTL = 60 * 60


class ArticleUserNoAttitudeCacheTTL(BaseCacheTTL):
    """
    用户对文章无态度缓存
    为解决缓存击穿，有效期不宜过长
    """
    TTL = 3 * 60
    MAX_DELTA = 30


class UserArticlesCacheTTL(BaseCacheTTL):
    """
    用户文章作品缓存时间，秒
    """
    TTL = 10 * 60
    MAX_DELTA = 2 * 60


class UserArticleCollectionsCacheTTL(BaseCacheTTL):
    """
    用户文章收藏缓存时间，秒
    """
    TTL = 10 * 60
    MAX_DELTA = 2 * 60


class ArticleCommentsCacheTTL(BaseCacheTTL):
    """
    文章评论列表缓存时间，秒
    """
    TTL = 30 * 60


class CommentRepliesCacheTTL(BaseCacheTTL):
    """
    评论回复列表缓存时间，秒
    """
    TTL = 30 * 60


class CommentCacheTTL(BaseCacheTTL):
    """
    评论信息缓存时间，秒
    """
    TTL = 30 * 60


class CommentNotExistsCacheTTL(BaseCacheTTL):
    """
    评论不存在结果缓存
    为解决缓存击穿，有效期不宜过长
    """
    TTL = 5 * 60
    MAX_DELTA = 60


class AnnouncementDetailCacheTTL(BaseCacheTTL):
    """
    系统公告详细信息缓存时间，秒
    """
    TTL = 2 * 60 * 60


class AnnouncementNotExistsCacheTTL(BaseCacheTTL):
    """
    公告不存在结果缓存
    为解决缓存击穿，有效期不宜过长
    """
    TTL = 5 * 60
    MAX_DELTA = 60