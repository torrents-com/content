"""
crawls a site known by the learner
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

class SiteSpider(TorrentsSpider):
    name = 'site'
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
 
        self.site = [urlsplit(url).netloc for url in self.start_urls][0].replace("www.","")
        
        self.max_links = max_links  # max num of links to follow per page
        self.max_offsite_ttl = max_offsite_ttl  # time-to-live jumps offsite
        self.max_depth = max_depth  # max depth it will go
        
        self.torrent_stores = cfg.get("parameters", "torrent_stores").split(",")
        
        
        self.main_tbl_src = self.get_table_source(self.site)
        
        self.load_torrent_pages()


    def start_requests(self):
        self.allowed_domains = [urlsplit(url).netloc for url in self.start_urls]

        for url in self.start_urls:
            yield Request(url)
            
    def load_torrent_pages(self):
        self.urls_torrent_page = None
        if self.site:
            domain = self.db.domain.find_one({"_id":self.site})
            if not domain is None and 'tp' in domain:
                self.urls_torrent_page = domain["tp"]
        
        if not self.urls_torrent_page:
            self.torrent_page_format = None
            return
        self.torrent_page_format = {}
        for url in self.urls_torrent_page:
            if not 'startswith' in self.torrent_page_format:
                self.torrent_page_format['startswith'] = url
                self.torrent_page_format['endswith'] = url
                continue
            pos = 0
            for c in self.torrent_page_format['startswith']:
                if c != url[pos]:
                    self.torrent_page_format['startswith'] = url[:pos]
                    break
                pos += 1
            
            self.torrent_page_format['startswith'] = self.torrent_page_format['startswith'].replace("www.","")
            
            pos = len(url)-1
            for c in self.torrent_page_format['endswith'][::-1]:
                if c != url[pos]:
                    self.torrent_page_format['endswith'] = url[pos+1:]
                    break
                pos -= 1
            
            
        self.torrent_page_format['count'] = len(self.urls_torrent_page[0].split("/"))
        
    def is_torrent_page(self, url, verbose = False):
        url = url.replace("www.","")
        if not self.torrent_page_format:
            if verbose:
                print "unknow"
            #unknow
            return True
        
        if verbose:
            print "@"*22
            print url
            rt = url.startswith(self.torrent_page_format['startswith']) and \
                url.endswith(self.torrent_page_format['endswith']) and \
                self.torrent_page_format['count'] == len(url.split("/"))
            print url.startswith(self.torrent_page_format['startswith'])
            print url.endswith(self.torrent_page_format['endswith'])
            print self.torrent_page_format['count'] == len(url.split("/"))
            print "@"*22
            #~ if rt:
                #~ print url
                #~ exit()
            return rt
        else:
            
            
            return url.startswith(self.torrent_page_format['startswith']) and \
                url.endswith(self.torrent_page_format['endswith']) and \
                self.torrent_page_format['count'] == len(url.split("/"))

    def like_torrent(self, url):
        return url.endswith(".torrent") or "/torrent" in url or "/download/" in url

    def parse_sentry(self, response):
        "Return list of new requests or items"
        
        #~ print "PARSE", response.url
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
        path = url_parts.path

        discovery_site = urlsplit(response.meta['url_discovery']).netloc
        # will be used in the stats

        is_torrent_page = self.is_torrent_page(response.url)
        
        #~ print "IS_TORRENT_PAGE", is_torrent_page
        
        #~ if is_torrent_page:
            #~ print response.url
            #~ exit()
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
                    me = MetaExtractor(response.meta['url_discovery'])
                    extract = me.extract()
                    #try 2 jumps back
                    if not extract and response.meta['url_discovery2'] != response.meta['url_discovery2']:
                        me = MetaExtractor(response.meta['url_discovery2'])
                        extract = me.extract()
                    
                    
                    if extract:
                        if ("size" in extract and ((float(extract['size']) / size)>1.01 or (float(extract['size']) / size)<0.99)) or   \
                            ("infohash" in extract and extract['infohash'].lower() != info['infohash'].lower()):
                            extract = {}
                        else:
                            if 'title' in extract:
                                #the words of title must be in the torrent
                                try:
                                    words = [w for w in re.findall(r"[\w']+", extract['title']) if len(w)>3]
                                    words_torrent = list(set(w for w in re.findall(r"[\w']+", "%s %s" % \
                                                    (info['filedir'] if "filedir" in info else "", info['filepaths'])) if len(w)>3))
                                    
                                    if  sum(w in words_torrent for w in words) < len(words) / 2:
                                        extract = {}
                                except UnicodeDecodeError:
                                    extract = {}
                            
                        for k, v in extract.items():
                            known_data[2][k if k != "image" else "thumbnail"] = v.strip()
                    
                    if extract:
                        #only if has metadata
                        for item in scrap_final(response, self.crawler.stats, self.db, (self.main_tbl_src, self.tbl_src), known_data):
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
        
        finals = scrap_final(response, self.db, self.crawler.stats, (self.main_tbl_src, self.tbl_src), add_candidates=False)
        if finals:
            for item in finals:
                yield item
            return

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

        known_links = None
        #~ if len(magnets) == 1:
        
        
        if is_torrent_page:
            
            me = MetaExtractor(response.url)
            known_links_dirty = me.get_links()
            known_links = None
            if known_links_dirty:
                known_links = [l for l in known_links_dirty \
                            if l.endswith('.torrent') or '/torrent/' in l]
            
            #~ print "KNWON_LINKS", known_links
            
            if not known_links:
                
                #maybe torrent page
                for magnet in magnets:
                    
                    #~ print "MAGNET", magnet
                    
                    me = MetaExtractor(response.url)
                    extract = me.extract()
                    
                    
                    
                    if not extract is None:
                        fake_resp, known_data = magnet2resp(magnet, response.url)
                        
                        #TODO: match ihs
                        if 'infohash' in extract and 'torrent:infohash' in known_data[2] and extract['infohash'].lower() == known_data[2]['torrent:infohash'].lower():
                            for k, v in extract.items():
                                known_data[2][k] = v.strip()
                            #~ for k, v in known_data[2].items():
                                #~ print k, v
                                
                            for item in scrap_final(fake_resp, self.crawler.stats, self.db, (self.main_tbl_src, self.tbl_src), known_data):
                                yield item
                            
                        torrents_links = {x for x in all_links if x.split("?")[0].endswith('.torrent')}
                        all_links = torrents_links
                
            # but don't "return", we continue to yield stuff!
        all_links -= magnets
        

        # Add links guessed, not present in @href's
        #disabled to avoid cross crawling
        #all_links |= set(re.findall(self.link_pattern, response.body))
        

        # Only take a subset if too many
        if len(all_links) > self.max_links:
            links = set(sample(all_links, self.max_links))
        else:
            links = all_links
        # But make sure to take all the ones that look like a torrent
        if is_torrent_page:
            
            if known_links:
                links = set(known_links)
                
            else:
                #Only torrents in pages with metadata
                links |= set(x for x in all_links \
                             if x.endswith('.torrent') or '/torrent/' in x)
            
        #make sure all pages with metadata
        links |= set(x for x in all_links if self.is_torrent_page(x))
        
        

        # Construct the new requests
        for link in links:
            is_torrent = site in self.torrent_stores or self.like_torrent(link)
            
            #~ print "IS_TORRENT", is_torrent, link
            
            if is_torrent and not is_torrent_page and not site in self.torrent_stores:
                continue
            
            new_url = urljoin("http://%s/"%self.site, link.replace("https://", "http://"))
            
            if is_torrent_page and not (is_torrent or self.is_torrent_page(new_url)):
                continue
                
            
            excepts = ["jpg", "jpeg", "gif", "png", "js", "css"]            
            if any(link.lower().endswith(".%s"%e) for e in excepts):
                continue
            
            # Torrent stores
            if site in self.torrent_stores and (not ":" in link or not ":" in response.url and not response.url.split(":",1)[1] in link.split(":",1)[1]):
                continue
                
            url = urljoin(response.url, link)
            
            
            # Not interesting
            blacklist = ["/ads", "imdb.com", "static.", "twitter.com", "facebook.com", "amazon.", "youtube.com"]
            
            
            if any([w in url for w in blacklist]):
                continue
            
            
            if site != self.site and not site in self.torrent_stores:
                continue
                
                
            ttl = self.max_offsite_ttl
        
            
            
            #~ print "YIELD", "%s(%s)" % (response.url,is_torrent_page), "->",  "%s(%s)" % (new_url, is_torrent), 10 if is_torrent else 9 if self.is_torrent_page(new_url) else -1
            yield Request(new_url,
                          meta={'offsite_ttl': ttl,
                                #~ 'url_discovery': response.url},
                                'url_discovery': response.url if site == self.site else response.meta['url_discovery'],
                                'url_discovery2': response.meta['url_discovery'] if site == self.site else response.meta['url_discovery2']},
                          priority=10 if is_torrent else 9 if self.is_torrent_page(new_url) else randint(0, 8), dont_filter=is_torrent)

