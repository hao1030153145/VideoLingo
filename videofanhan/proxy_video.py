from flask import Flask, request, send_from_directory
import requests
import os
from werkzeug.middleware.proxy_fix import ProxyFix
app = Flask(__name__, static_folder='.')
app.wsgi_app = ProxyFix(app.wsgi_app)
# 配置代理目标
PROXY_PATH = '/api/'
PROXY_TARGET = 'http://a.fanhan-ai.com:18501'
@app.route(PROXY_PATH + '<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy(path):
    """代理 API 请求"""
    url = f"{PROXY_TARGET}{PROXY_PATH}{path}"

    # 转发请求方法、头部和数据
    resp = requests.request(
        method=request.method,
        url=url,
        headers={key: value for key, value in request.headers if key != 'Host'},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        stream=True
    )

    # 获取响应头部
    headers = [(name, value) for name, value in resp.raw.headers.items()]

    # 返回响应
    return resp.content, resp.status_code, headers
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_static(path):
    """提供静态文件，对于SPA应用支持回退到index.html"""
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8502)