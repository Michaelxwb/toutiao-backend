from qiniu import Auth, put_file, etag, put_data
import qiniu.config
from flask import current_app


def pic_upload(pic_data):
    """
    将用户二进制图片数据上传到七牛云存储平台
    :return: 图片的名称
    """

    # 需要填写你的 Access Key 和 Secret Key
    access_key = current_app.config["QINIU_ACCESS_KEY"]
    secret_key = current_app.config["QINIU_SECRET_KEY"]

    # 构建鉴权对象,验证用户身份
    q = Auth(access_key, secret_key)

    # 要上传的空间名称
    bucket_name = current_app.config["QINIU_BUCKET_NAME"]

    # 上传后到七牛云保存的图片名称
    # key = 'my-python-logo.png'
    # 如果该字段设置为None,七牛云就会自动分配一个唯一图片名称
    key = None

    # 生成上传 Token，可以指定过期时间等 ，token默认过期时长1小时 ---坑
    token = q.upload_token(bucket_name, key, 36000)

    # 要上传二进制文件的到七牛云存储平台
    ret, info = put_data(token, key, pic_data)

    # {'hash': 'FpymV3ZlaGQJcLhmu1CSGIK1bsUT', 'key': 'FpymV3ZlaGQJcLhmu1CSGIK1bsUT'}
    # key图片名称
    print(ret)
    print("=========")
    # _ResponseInfo__response:<Response [200]>, exception:None, status_code:200,
    # text_body:{"hash":"FpymV3ZlaGQJcLhmu1CSGIK1bsUT","key":"FpymV3ZlaGQJcLhmu1CSGIK1bsUT"},
    # req_id:zrkAAABVXge417AV, x_log:X-Log
    print(info)

    if ret is None:
        return None
    # 返回图片名称
    return ret["key"]
