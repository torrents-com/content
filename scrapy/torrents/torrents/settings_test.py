# Settings that will be used in test mode only. That is, when
# ../launch.py is called with the --test option.
#
# By setting the SCRAPY_SETTINGS_MODULE env var this settings can be
# used instead of the default settings.py

BOT_NAME = 'torrents_test'
BOT_VERSION = '1.0'

SPIDER_MODULES = ['torrents.spiders']
NEWSPIDER_MODULE = 'torrents.spiders'
USER_AGENT = '%s/%s' % (BOT_NAME, BOT_VERSION)

LOG_LEVEL = 'DEBUG'  # 'INFO'

# Max simultaneous requests that will be performed to any single domain
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# let's be nice...

# Activate my item pipeline
ITEM_PIPELINES = ['torrents.pipelines.TorrentsPipeline']

# Choose my spider middleware
SPIDER_MIDDLEWARES = {
    'torrents.middlewares.MemoryMiddleware': 543,
    'torrents.middlewares.FilterUrlPathMiddleware': 400,
    }

# Don't hit the server too hard
DOWNLOAD_DELAY = 2

DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': None,
    'torrents.middlewares.MultiProxyMiddleware': 800,
    'torrents.middlewares.FilterRequestsMiddleware': 420,
    }

DOWNLOAD_HANDLERS = {
    'magnet': 'torrents.handlers.MagnetDownloadHandler',
    }

TELNETCONSOLE_ENABLED = False

WEBSERVICE_ENABLED = False

#COOKIES_DEBUG = True
