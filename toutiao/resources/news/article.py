import time

from flask import current_app, g
from flask_restful import Resource, reqparse, inputs
from flask_restful.reqparse import RequestParser

from cache.article import ArticleInfoCache
from sqlalchemy.exc import DatabaseError
from cache import article as cache_article
from rpc import reco_pb2, reco_pb2_grpc


class ArticleListResource(Resource):
    '''通过自己封装的工具类获取某篇新闻的信息'''

    def _feed_articles(self, channel_id, timestamp, feed_count):
        # 连接grpc服务器
        channel = current_app.rpc_reco_channel

        # 获取stub对象
        stub = reco_pb2_grpc.UserArticleRecommendStub(channel)

        # 组织请求对象
        request = reco_pb2.UserRequest()
        # 首页不要求用户强制登录  没有登录设置匿名用户: "annoy"
        request.user_id = str(g.user_id) if g.user_id else "annoy"
        request.channel_id = channel_id
        request.article_num = feed_count
        request.time_stamp = timestamp

        article_response = stub.user_recommend(request)

        return article_response.recommends, article_response.time_stamp

    def get(self):
        """
        获取文章列表
        """

        # 请求解析对象
        qs_parser = RequestParser()
        # 频道id
        qs_parser.add_argument('channel_id', type=int, required=True, location='args')
        # 时间戳
        qs_parser.add_argument('timestamp', type=inputs.positive, required=True, location='args')
        # 开启参数解析
        args = qs_parser.parse_args()

        channel_id = args.channel_id
        timestamp = args.timestamp
        # 推荐文章数量
        per_page = 10

        try:
            feed_time = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(time.time()))
        except Exception:
            return {'message': 'timestamp param error'}, 400

        results = []

        # TODO: 调用grpc 获取推荐文章列表
        feeds, pre_timestamp = self._feed_articles(channel_id, timestamp, per_page)

        # 查询文章
        for feed in feeds:
            # feed 代表的 Article 文章
            # feed.article_id 文章id

            # 从缓存工具类中获取文章数据

            article = cache_article.ArticleInfoCache(feed.article_id).get()

            # 文章对象
            if article:
                article['pubdate'] = feed_time
                # 埋点参数
                article['trace'] = {
                    'click': feed.track.click,
                    'collect': feed.track.collect,
                    'share': feed.track.share,
                    'read': feed.track.read
                }
                results.append(article)

        return {'pre_timestamp': pre_timestamp, 'results': results}
