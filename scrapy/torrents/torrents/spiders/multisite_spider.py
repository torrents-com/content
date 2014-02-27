"""
Multiple sites at once
"""

import re, pymongo

from pprint import pprint
from urllib import unquote

from urlparse import urljoin, urlsplit
from random import randint, sample

from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy.exceptions import NotConfigured

from torrents.webhosts import scrap_final
from torrents.scrap_files import torrent_info, magnet2resp
from bencode import BTFailure

from scrapy.spider import BaseSpider
from torrents_spider import TorrentsSpider

from learn.meta_extractor import MetaExtractor

from ConfigParser import ConfigParser
import os

class MultisiteSpider(TorrentsSpider):
    name = 'multisite'
    #allowed_domains = []  #
    # Instead of start_urls, we have a start_request() function defined
    test_whitelist = []

    def __init__(self, start_urls=None,
                 max_links=200, max_offsite_ttl=1, max_depth=50, db_conn = None, 
                 *args, **kwargs):
        # Initialize parent class
        super(SiteSpider, self).__init__(*args, **kwargs)
        cfg = ConfigParser()
        cfg.readfp(open(os.path.join(os.path.dirname(__file__), '..', 'scrapy.properties')))
        # Now our local initialization
        
        # Expression used to find links not inside @href's
        self.link_pattern = re.compile('http://[^"\'><\n\t ]+')
        # Read urls from either the database or the option
        
        
        if start_urls is None:
            raise Exception("start_urls None not permited")
        self.start_urls = start_urls.split(',')
        
        #~ self.site = [urlsplit(url).netloc for url in self.start_urls][0].replace("www.","")
        
        self.sites = [urlsplit(url).netloc.replace("www.","") for url in self.start_urls]
        
        
        self.site = "_".join(s[0] for s in self.sites).replace(".", "_") 
        
        self.max_links = max_links  # max num of links to follow per page
        self.max_offsite_ttl = max_offsite_ttl  # time-to-live jumps offsite
        self.max_depth = max_depth  # max depth it will go
        
        self.torrent_stores = cfg.get("parameters", "torrent_stores").split(",")
        
        
        #~ self.main_tbl_src = self.get_table_source(self.site)
        self.main_tbl_src = {s:self.get_table_source(s) for s in self.sites}


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
    
        if 'url_discovery2' not in response.meta:
            response.meta['url_discovery2'] = 'unknown'
    
        if 'url_discovery' not in response.meta:
            response.meta['url_discovery'] = 'unknown'
        
            
        # Shortcuts
        url_parts = urlsplit(response.url)
        site = url_parts.netloc.replace("www.","")
        if not site in self.main_tbl_src:
            self.main_tbl_src[site] = self.get_table_source(site)
        path = url_parts.path

        discovery_site = urlsplit(response.meta['url_discovery']).netloc
        # will be used in the stats

        
        # See what to do depending on the type
        if 'Content-Type' in response.headers:
            content = response.headers['Content-Type'].split(';')[0].lower()
            
            
            if content in ['text/html', 'text/xml', 'application/xhtml+xml']:
                pass
                
            if content == 'application/x-bittorrent' or \
                    content == 'application/octet-stream' or \
                    content == 'application/force-download':
                        
                
                try:
                    info = torrent_info(response.body)
                    
                    size = info['size']
                    del info['size']
                    known_data = [unquote(path.split('/')[-1]).decode('utf8'), size, info]
                    print "Extrayendo de %s" % response.meta['url_discovery']
                    me = MetaExtractor(response.meta['url_discovery'])
                    extract = me.extract()
                    #try 2 jumps back
                    if not extract and response.meta['url_discovery2'] != response.meta['url_discovery2']:
                        print "Extrayendo(2) de %s" % response.meta['url_discovery']
                        me = MetaExtractor(response.meta['url_discovery2'])
                        extract = me.extract()
                    
                    
                    
                    
                    if extract:
                        if ("size" in extract and extract['size'] != size) or   \
                            ("infohash" in extract and extract['infohash'] != info['infohash']):
                            extract = {}
                        for k, v in extract.items():
                            known_data[2][k if k != "image" else "thumbnail"] = v.strip()
                    
                    
                    for item in scrap_final(response, self.crawler.stats, self.db, (self.main_tbl_src[site], self.tbl_src), known_data):
                        yield item
                    #~ else:
                        #~ if "/torrent/" in response.meta['url_discovery']:
                        #~ print "No se pudo extraer de ", response.meta['url_discovery']
                        #~ exit()
                    return
                    
                except BTFailure:
                    pass
                    # we probably got a application/octet-stream that is
                    # not a torrent file
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
        
        
        finals = scrap_final(response, self.db, self.crawler.stats, (self.main_tbl_src[site], self.tbl_src), add_candidates=False)
        if finals:
            for item in finals:
                yield item
            return

        # More shortcuts
        try:
            sel = HtmlXPathSelector(response).select  # for short notation
            
             # Find all the links
            all_links_raw = set(sel('//@href').extract())
            
            #only the first torrent to avoid cross crawling
            all_links_ = []
            torrent = False
            for l in all_links_raw:
                if not l.endswith('.torrent') or not torrent:
                    all_links_.append(l)
                if not torrent:
                    torrent = l.endswith('.torrent')
                    
            all_links = set(all_links_)
            
            
            
            # Or maybe it would be better to do
            #   all_links = set(sel('//a[not(@rel="nofollow")]/@href').extract())
            # but doesn't look so from http://en.wikipedia.org/wiki/Nofollow

            # It would be great to sort them in some sort of "possible
            # interest" way

            # First, magnet links if any
        except AttributeError:
            all_links = set()
        
        magnets = {x for x in all_links if x.startswith('magnet:?')}

        if len(magnets) == 1:
            #maybe torrent page
            for magnet in magnets:
                print "Extrayendo(M) de %s" % response.url
                me = MetaExtractor(response.url)
                extract = me.extract()
                if not extract is None:
                    fake_resp, known_data = magnet2resp(magnet, response.url)
                    for k, v in extract.items():
                        known_data[2][k] = v.strip()
                    #~ for k, v in known_data[2].items():
                        #~ print k, v
                        
                    print known_data
                    print "-"*32

                    for item in scrap_final(fake_resp, self.crawler.stats, self.db, (self.main_tbl_src[site], self.tbl_src), known_data):
                        yield item
                        
                    #remove all torrents to avoid cross crawling
                    torrents_links = {x for x in all_links if x.endswith('.torrent')}
                    all_links -= torrents_links
            
                    
            # but don't "return", we continue to yield stuff!
        all_links -= magnets

        # Add links guessed, not present in @href's
        #disabled to avoid cross crawling
        #all_links |= set(re.findall(self.link_pattern, response.body))
        

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
            
            
            #~ if site == self.site or site in self.torrent_stores:
            if site in self.sites or site in self.torrent_stores:
                ttl = self.max_offsite_ttl
            else:
                ttl = response.meta.get('offsite_ttl', self.max_offsite_ttl) - 1
        
            
            #~ yield Request(urljoin("http://%s/"%self.site, link.replace("https://", "http://")),
                          #~ meta={'offsite_ttl': ttl,
                                #~ 'url_discovery': response.url if site == self.site else response.meta['url_discovery'],
                                #~ 'url_discovery2': response.meta['url_discovery'] if site == self.site else response.meta['url_discovery2']},
                          #~ priority=10 if site in self.torrent_stores or link.endswith(".torrent") else randint(0, 10), dont_filter=True)
            yield Request(urljoin("http://%s/"%site, link.replace("https://", "http://")),
                          meta={'offsite_ttl': ttl,
                                'url_discovery': response.url if site in self.sites else response.meta['url_discovery'],
                                'url_discovery2': response.meta['url_discovery'] if site in self.sites else response.meta['url_discovery2']},
                          priority=10 if site in self.torrent_stores or link.endswith(".torrent") else randint(0, 10), dont_filter=True)

