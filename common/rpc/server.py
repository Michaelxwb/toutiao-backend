import time

import grpc
import reco_pb2_grpc
import reco_pb2
from concurrent.futures import ThreadPoolExecutor


class UserArticleRecommendServicer(reco_pb2_grpc.UserArticleRecommendServicer):

    def user_recommend(self, request, context):
        # 接收web后端发送过来的请求
        user_id = request.user_id
        channel_id = request.channel_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 调用系统推荐方法获取推荐文章数据

        # 返回推荐的文章响应对象
        article_response = reco_pb2.ArticleResponse()
        # 设置曝光数据
        article_response.exposure = "exposure message"
        # 设置时间戳
        article_response.time_stamp = round(time.time() * 1000)

        article_list = []

        for i in range(article_num):
            article = reco_pb2.Article()
            article.article_id = i + 1
            # 埋点数据
            article.track.click = "click action {}".format(i + 1)
            article.track.collect = "collect action {}".format(i + 1)
            article.track.share = "share action {}".format(i + 1)
            article.track.read = "read action {}".format(i + 1)
            article_list.append(article)

        article_response.recommends.extend(article_list)

        return article_response


def server():
    # 通过grpc创建服务器对象
    server = grpc.server(ThreadPoolExecutor(max_workers=10))

    # 将推荐文章服务加入到服务器对象中
    reco_pb2_grpc.add_UserArticleRecommendServicer_to_server(UserArticleRecommendServicer(), server)

    # 绑定ip端口并开启服务
    server.add_insecure_port('127.0.0.1:8888')
    server.start()

    # 阻塞进程防止程序退出
    while True:
        time.sleep(10)


if __name__ == '__main__':
    server()
