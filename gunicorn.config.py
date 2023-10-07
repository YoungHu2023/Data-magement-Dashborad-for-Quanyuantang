import multiprocessing

bind = '127.0.0.1:8000'
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'uvicorn.workers.UvicornWorker'
timeout=60

# 指定每个工作者的线程数
threads = 1

# 监听队列
backlog = 2048

# 设置最大并发量
worker_connections = 1000

# 默认False，设置守护进程，将进程交给supervisor管理
daemon = False

debug = True

loglevel = 'debug'

# 默认None，这会影响ps和top。如果要运行多个Gunicorn实例，
# 需要设置一个名称来区分，这就要安装setproctitle模块。如果未安装
proc_name = 'main'

# 预加载资源
preload_app = True

autorestart = True
