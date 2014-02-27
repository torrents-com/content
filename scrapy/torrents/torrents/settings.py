# Scrapy settings for a generic torrents project
#
# See http://doc.scrapy.org/en/latest/topics/settings.html

BOT_NAME = 'torrents'
BOT_VERSION = '1.1'

SPIDER_MODULES = ['torrents.spiders']
NEWSPIDER_MODULE = 'torrents.spiders'
USER_AGENT = '%s/%s' % (BOT_NAME, BOT_VERSION)


#
# Our specific settings here
#

# Don't be so verbose, pretty please
LOG_LEVEL = 'DEBUG'  # 'INFO'

# Max simultaneous requests that will be performed to any single domain
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# let's be nice...

# Activate my item pipeline
ITEM_PIPELINES = ['torrents.pipelines.TorrentsPipeline']

# Activate extensions - so far, one that limits the used memory
EXTENSIONS = {
    'scrapy.contrib.memusage.MemoryUsage': 500,
}

MEMUSAGE_LIMIT_MB = 1000  # kill spiders that use more than 3 GB!

# Choose my spider middleware (which remembers visited urls)
#~ SPIDER_MIDDLEWARES = {
    #~ 'torrents.middlewares.MemoryMiddleware': 543,
#~ }


#Activate in production
#~ DEPTH_PRIORITY = 1
#~ SCHEDULER_DISK_QUEUE = 'scrapy.squeue.PickleFifoDiskQueue'
#~ SCHEDULER_MEMORY_QUEUE = 'scrapy.squeue.FifoMemoryQueue'
#see scrapy_controler.py
#~ JOBDIR = ""

# Don't hit the server too hard
DOWNLOAD_DELAY = 2 #0.2 #40.0 #2.40

# Override the proxy middleware (so we can use multiple proxies)
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': None,
    'torrents.middlewares.MultiProxyMiddleware': 800,
}

# Know how to handle urls that start with "magnet:"
DOWNLOAD_HANDLERS = {
    'magnet': 'torrents.handlers.MagnetDownloadHandler',
}
