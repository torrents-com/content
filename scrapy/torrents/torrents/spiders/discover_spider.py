"""
discover new torrent's sites
"""

import re, pymongo

from pprint import pprint

from urlparse import urljoin, urlsplit
from random import randint, sample

from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy.exceptions import NotConfigured

from torrents.webhosts import scrap_final
from torrents.scrap_files import torrent_info, magnet2resp
from bencode import BTFailure
from random import randrange

from scrapy.spider import BaseSpider
from torrents_spider import TorrentsSpider

from ConfigParser import ConfigParser
import os

class DiscoverSpider(TorrentsSpider):
    name = 'discover'
    #allowed_domains = []  #
    # Instead of start_urls, we have a start_request() function defined
    test_whitelist = []

    def __init__(self, start_urls=None,
                 max_links=500, max_offsite_ttl=10, max_depth=10, db_conn = None, 
                 *args, **kwargs):
        # Initialize parent class
        super(DiscoverSpider, self).__init__(*args, **kwargs)
        cfg = ConfigParser()
        cfg.readfp(open(os.path.join(os.path.dirname(__file__), '..', 'scrapy.properties')))
        # Now our local initialization
        
        # Expression used to find links not inside @href's
        self.link_pattern = re.compile('http://[^"\'><\n\t ]+')
        # Read urls from either the database or the option
        
        
        self.start_urls = ["http://%s"%k for k in self.tbl_src if "." in k]
 
        #~ self.site = [urlsplit(url).netloc for url in self.start_urls][0].replace("www.","")
        
        self.max_links = max_links  # max num of links to follow per page
        self.max_offsite_ttl = max_offsite_ttl  # time-to-live jumps offsite
        self.max_depth = max_depth  # max depth it will go
        
        self.torrent_stores = cfg.get("parameters", "torrent_stores").split(",")
        for store in self.torrent_stores:
            if "http://%s"%store in self.start_urls:
                self.start_urls = ",".join(self.start_urls).replace("http://%s,"%store,"").split(",")
                
        self.start_urls += ",http://www.bing.com/search?q=torrent&first=%d1,http://www.bing.com/search?q=torrent&first=%d1" % (randrange(10), randrange(10)) 
        self.start_urls += ",https://search.yahoo.com/search?ei=UTF-8&p=torrent&n=100,https://search.yahoo.com/search?ei=UTF-8&p=torrents&n=100"
        
        #ensure table domain
        for src in self.tbl_src:
            self.save_domain(src)
            
            
        
        #~ self.main_tbl_src = self.get_table_source(self.site)

    def save_domain(self, src):
        if not "." in src:
            return
        src = ".".join(src.split(".")[-2:])
        if not self.db.domain.find_one({"_id":src}):
            self.db.domain.save({"_id":src})
            self.start_urls.append(src)

    def start_requests(self):
        self.allowed_domains = [urlsplit(url).netloc for url in self.start_urls]

        for url in self.start_urls:
            yield Request(url)

    def parse_sentry(self, response):
        "Return list of new requests or items"
        
        # Offsite Time-To-Live
        if response.meta.get('offsite_ttl') == 0:
            return
            
        # Depth
        if response.meta.get('depth', 0) > self.max_depth:
            return
            
        # Increment the stats
        self.crawler.stats.inc_value('visited')

        # Initialize url_discovery if not present
    
        # Shortcuts
        url_parts = urlsplit(response.url)
        site = url_parts.netloc.replace("www.","")
        path = url_parts.path
        
        site_candidate = not "http://" + site in self.start_urls and not "http://www." + site in self.start_urls
        
        
        # See what to do depending on the type
        if 'Content-Type' in response.headers:
            content = response.headers['Content-Type'].split(';')[0].lower()
            
            
            if content in ['text/html', 'text/xml', 'application/xhtml+xml']:
                pass
                
            if content == 'application/x-bittorrent' and site_candidate:
            
                #torrent site. save in db
                
                self.start_urls.append(url_parts.netloc)
                return
                
            elif content in ['text/javascript', 'application/javascript',
                             'application/x-javascript', 'text/css',
                             'image/gif', 'image/png']:
                return
            else:
                # We will try to use scrap_final and see if it's a file
                # Crazy maybe?
                pass
                #raise RuntimeError('Got unknown content type: %s' % content)
        #~ else:
            #~ print response.url , " Sin content-type"

        # Send the last links if available
        
        
        # More shortcuts
        try:
            sel = HtmlXPathSelector(response).select  # for short notation
            
             # Find all the links
            all_links = set(sel('//@href').extract())
            # Or maybe it would be better to do
            #   all_links = set(sel('//a[not(@rel="nofollow")]/@href').extract())
            # but doesn't look so from http://en.wikipedia.org/wiki/Nofollow

            # It would be great to sort them in some sort of "possible
            # interest" way

            # First, magnet links if any
        except AttributeError:
            all_links = set()
        
        magnets = {x for x in all_links if x.startswith('magnet:?')}

        if site_candidate and len(magnets) > 0:
            #torrent site. save in db
            self.save_domain(url_parts.netloc)
            return
        
        all_links -= magnets

        # Add links guessed, not present in @href's
        all_links |= set(re.findall(self.link_pattern, response.body))
        

        # Only take a subset if too many
        if len(all_links) > self.max_links:
            links = set(sample(all_links, self.max_links))
            # But make sure to take all the ones that look like a torrent
            links |= set(x for x in all_links \
                         if x.endswith('.torrent') or '/torrent/' in x)
        else:
            links = all_links

        # Construct the new requests
        for link in links:
            
            
            excepts = ["jpg", "jpeg", "gif", "png", "js", "css"]            
            if any(link.lower().endswith(".%s"%e) for e in excepts):
                continue
            
            # Torrent stores
            if site in self.torrent_stores and (not ":" in link or link.split(":",1)[1] != response.url.split(":",1)[1]):
                continue
                
            url = urljoin(response.url, link)
            
            # Not interesting
            blacklist = ["/ads", "imdb.com", "static.", "twitter.com", "facebook.com", "amazon.", "youtube.com"]
            
            if any([w in url for w in blacklist]):
                continue
            
            ttl = response.meta.get('offsite_ttl', self.max_offsite_ttl) - 1
            
            new_url = urljoin("http://%s/"%site, link.replace("https://", "http://"))
            new_site = urlsplit(new_url).netloc.replace("www.","")
            
            if "http://" + new_site in self.start_urls or "http://www." + new_site in self.start_urls:
                continue
            
            yield Request(new_url,
                          meta={'offsite_ttl': ttl},
                          priority=randint(0, 10), dont_filter=True)

