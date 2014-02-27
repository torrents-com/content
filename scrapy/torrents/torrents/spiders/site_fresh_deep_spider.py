"""
like fresh, change default max_depth 
"""

from torrents.spiders.site_fresh_spider import SiteFreshSpider

class SiteFreshDeepSpider(SiteFreshSpider):
    name = 'site_fresh_deep'
    #allowed_domains = []  #
    # Instead of start_urls, we have a start_request() function defined
    test_whitelist = []

    def __init__(self, start_urls=None,
                 max_links=300, max_offsite_ttl=1, max_depth=2, db_conn = None, 
                 *args, **kwargs):
        # Initialize parent class
        super(SiteFreshDeepSpider, self).__init__(start_urls,
                 max_links, max_offsite_ttl, max_depth, db_conn,*args, **kwargs)
