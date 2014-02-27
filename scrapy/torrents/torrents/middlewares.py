# Spider and Downloader Middlewares. See settings.py for how to switch
# them on/off.
#
# JBC. Feb 2012.

import re
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
from urlparse import urlsplit

from scrapy.http import Request
from scrapy import log


#  ************************************************************************
#  *                                                                      *
#  *                          Spider Middlewares                          *
#  *                                                                      *
#  ************************************************************************

# Spider Middlewares: what happens just before & after a request is
# passed to/from the Spider.
#
# They have to be added to SPIDER_MIDDLEWARES in settings
# See: http://doc.scrapy.org/en/latest/topics/spider-middleware.html

class MemoryMiddleware(object):
    """
    Memory Spider Middleware
    
    Don't send a request to an url that we have already visited.
    """

    def __init__(self):
        self.visited = set()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if not isinstance(r, Request):
                yield r  # don't do anything for Item()s, only for Request()s
                continue
 
            # Generate a "site-id" that includes important parts to
            # identify a place: netloc, path, query and body (useful
            # for POST requests), but don't include "#fragment"...
            parts = urlsplit(r.url)
            site_id = parts.netloc + parts.path + parts.query + r.body
            #~ if "torcache" in site_id:
                #~ print site_id
                #~ print response.url
                #~ print response.meta['url_discovery']
                #~ print "-"*12
            
            if site_id in self.visited and not site_id == response.url.replace("http://", "").replace("https://", ""):
                pass  # don't go again to a visited place
            else:
                self.visited.add(site_id)  # a new place! remember it
                yield r


class FilterUrlPathMiddleware(object):

    """Only pass requests that go to a url in a whitelist."""

    # Used for testing the spiders.

    def process_spider_output(self, response, result, spider):
        for r in result:
            if not isinstance(r, Request):
                yield r  # don't do anything for Item()s, only for Request()s
                continue

            if not any(re.match(p, r.url) for p in spider.test_whitelist):
                pass  # only whitelisted urls will be yield, discard the rest
            else:
                yield r


#  ************************************************************************
#  *                                                                      *
#  *                        Downloader Middlewares                        *
#  *                                                                      *
#  ************************************************************************

# Downloader Middlewares: what happens just before & after a request
# is passed to the downloader or a response is taken from it.

from urllib import getproxies
# so we warn people if they try to use the default ones

from scrapy.exceptions import NotConfigured, IgnoreRequest
import random

class MultiProxyMiddleware(object):

    """Use a different proxy for each request."""

    # Made taking HttpProxyMiddleware as reference.

    def __init__(self):
        if getproxies() != {}:
            log.msg(('getproxies() not empty:\n  %s\n' % getproxies()) + \
                        '(probably from the *_proxy variables in the shell) ' \
                        'but we do not use any of those.', log.WARNING)

        cfg = ConfigParser()
        cfg.readfp(open('scrapy.properties'))
        self.proxies = cfg.get('remote', 'proxies').split()
        try:
            special = cfg.get('remote', 'proxy_special').split('\n')
            self.proxy_special = dict(x.split() for x in special if len(x) > 1)
        except NoOptionError:
            log.msg('No "proxy_special" option. Assuming empty.', log.WARNING)
            self.proxy_special = {}

        self.last_index = -1

        if self.proxies == []:
            raise NotConfigured

    def process_request(self, request, spider):
        # Ignore if proxy already set
        if 'proxy' in request.meta:
            return

        site = urlsplit(request.url).netloc
        if site in self.proxy_special:
            proxy = self.proxy_special[site]
        else:
            proxy = self._choose_proxy()

        request.meta['proxy'] = 'http://' + proxy

    def _choose_proxy(self):
        """A wise way to choose the next proxy."""

        # return random.choice(self.proxies)
        # # or maybe not so wise...

        self.last_index = (self.last_index + 1) % len(self.proxies)
        return self.proxies[self.last_index]


class FilterRequestsMiddleware(object):

    """Only pass requests that go to a url in a whitelist."""

    # Very similar to FilterUrlPathMiddleware, actually, but this
    # happens from the scheduler and before the downloader. So
    # basically it is to avoid all the start_urls when in test mode.

    def process_request(self, request, spider):
        if not any(re.match(p, request.url) for p in spider.test_whitelist):
            raise IgnoreRequest()
        else:
            return None  # continue processing this request normally


from torrents.settings import USER_AGENT

class MultiUserAgentMiddleware(object):

    """Use a different user agent for earch request."""

    def __init__(self):
        try:
            cfg = ConfigParser()
            cfg.readfp(open('scrapy.properties'))
            self.user_agents = cfg.get('spider', 'user_agents').split()
        except (NoSectionError, NoOptionError) as e:
            log.msg('Cannot read "user_agents" option. Using default.')
            self.user_agents = [USER_AGENT]

    def process_request(self, request, spider):
        request.headers.setdefault('User-Agent',
                                   random.choice(self.user_agents))
