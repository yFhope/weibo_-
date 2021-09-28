from loguru import logger

ck_pool = [
    "微博cookie1",
    "微博cookie2",
    "cookieN",
]

logger.add('./logs/my.log', rotation='03:00', format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module} | {message}",
           encoding='utf-8')
