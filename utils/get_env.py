import os


def get_env() -> dict:

    para = {}
    para['auth'] = os.environ.get('AUTH')
    para['db_host'] = os.environ.get('DB_HOST')
    para['db_user'] = os.environ.get('DB_USER')
    para['db_password'] = os.environ.get('DB_PASSWORD')
    para['db_name'] = os.environ.get('DB_NAME')
    para['db_PORT'] = os.environ.get('DB_PORT')
    if os.environ.get('DEBUG_MODE') == "True":
        para['debug_mode'] = True
    else:
        para['debug_mode'] = False
    para['per_page'] = os.environ.get('PER_PAGE')
    para['proxies'] = os.environ.get('PROXIES')

    para['auth'] = "Bearer 158125|LZ5cSvcoLfyMemmQa458UhRTn8gvEMmfpzriYUw2"
    para['debug_mode'] = True
    para['per_page'] = 50
    return para
