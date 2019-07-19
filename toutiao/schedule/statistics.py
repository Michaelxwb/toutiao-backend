from cache.persstorage import *


def fix_statistics(app):
    # 修正用户发布发文章数量
    with app.app_context():
        __fix_statistics(UserArticleCountStorage)
        __fix_statistics(UserArticleLikingCountStorage)
        __fix_statistics(UserFollowingsCountStorage)
        __fix_statistics(UserArticleCollectingCountStorage)
        __fix_statistics(ArticleDislikeCountStorage)
        __fix_statistics(CommentLikingCountStorage)
        # __fix_statistics(ArticleReadingCountStorage)
        # __fix_statistics(UserArticlesReadingCountStorage)
        __fix_statistics(ArticleCommentCountStorage)
        __fix_statistics(CommentReplyCountStorage)
        __fix_statistics(UserLikedCountStorage)
        __fix_statistics(UserFollowersCountStorage)


def __fix_statistics(cls_name):
    result = cls_name.db_query()
    cls_name.reset(result)
