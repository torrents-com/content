# Magnet handler. See settings.py for how to switch it on/off.
#
# JBC. Jul 2013.

from scrapy.http import TextResponse
from scrapy.utils.decorator import defers  # maybe we don't gain anything...


class MagnetDownloadHandler(object):
    def __init__(self, settings):
        pass

    @defers
    def download_request(self, request, spider):
        "Just return the magnet link in the url"
        return TextResponse(url=request.url)  # will get to the spider's parse()
