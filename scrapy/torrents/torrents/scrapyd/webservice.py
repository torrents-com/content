# The classes in this file are used in the project's scrapyd.conf
# file. They appear in the [services] section.
#
# This way, a request like http://localhost:16005/info will be handled
# by the resource specified in the class Info.

import traceback

from ConfigParser import ConfigParser
from json import loads, dumps
from copy import deepcopy

from pprint import pprint

from urllib2 import urlopen, URLError

from scrapy.utils.txweb import JsonResource


class WsResource(JsonResource):

    def __init__(self, root):
        JsonResource.__init__(self)
        self.root = root

    def render(self, txrequest):
        try:
            return JsonResource.render(self, txrequest)
        except Exception, e:
            if self.root.debug:
                return traceback.format_exc()
            print traceback.format_exc()
            r = {"status": "error", "message": str(e)}
            return self.render_object(r, txrequest)


def port(process):
    "Return the port of the webservice for the given ScrapyProcessProtocol"
    return [arg.split('=')[1] for arg in process.args \
                if arg.startswith('WEBSERVICE_PORT=')][0]

def args(proc):
    "Return the special args passed to the given ScrapyProcessProtocol"
    pos = [i+1 for i,arg in enumerate(proc.args) if arg == '-a']
    return [proc.args[i].replace('=', '_') for i in pos \
                if not proc.args[i].startswith('_job=')]

# We want names that can be passed as keys to zabbix, so they have to
# conform to:
# http://www.zabbix.com/documentation/1.8/manual/config/items
# And that means in particular that we can't use " ", ":" nor "=".

def name(process):
    "A name for the process, includes spider name and its args"
    return '-'.join([process.spider] + args(process))


class Info(JsonResource):
    isLeaf = True

    def __init__(self, root):
        self.root = root  # it looks as a WsResource this way...
        JsonResource.__init__(self)

    def render_GET(self, txrequest):
        # It expects something like {"1" : "alluc", "2" : "cinegratis", ...}
        running = self.root.launcher.processes.values()
        return {port(p):name(p) for p in running}


class Config(JsonResource):

    def __init__(self, root):
        self.root = root
        self.children = []  # so it knows there are no static children

        cfg = ConfigParser()
        cfg.readfp(open('scrapy.properties'))

        self.graph_default = loads(cfg.get('autodiscover', 'graph_default'))
        keys_default = loads(cfg.get('autodiscover', 'keys_default'))
        for k in self.graph_default['keys']:
            self.graph_default['keys'][k].update(keys_default)

        JsonResource.__init__(self)

    def getChild(self, path, request):
        return self

    def render_GET(self, txrequest):
        "Return config info about the running spiders"

        running = self.root.launcher.processes.values()
        n_spider = {port(p):name(p) for p in running}
        request = txrequest.path[len('/config/'):]

        if not request:
            return {'subdirs': n_spider.keys()}  # not required, but nice
        elif request not in n_spider:
            return {}

        d = deepcopy(self.graph_default)
        d['graph'].update({'name': n_spider[request],
                           'title': 'Scrapy spider for ' + n_spider[request]})
        return d


class Data(JsonResource):

    """Get data (stats) from spiders."""

    def __init__(self, root):
        self.root = root
        self.children = []  # so it knows there are no static children

        JsonResource.__init__(self)

    def getChild(self, path, request):
        return self

    def render_GET(self, txrequest):
        "Return info (stats) about the running spiders"

        running = self.root.launcher.processes.values()
        n_spider = {port(p):name(p) for p in running}
        request = txrequest.path[len('/data/'):]
        if not request:
            return {'subdirs': n_spider.keys()}
        elif request not in n_spider:
            return {}

        data = dumps({'method': 'get_stats', 'id': 1})
        return loads(urlopen('http://localhost:%s/stats' % request,
                             data=data).read())['result']


class GetSettings(JsonResource):

    """Send the configuration data used to generate the graphs."""

    def __init__(self, root):
        """Save locally the configuration from scrapy.properties."""
        cfg = ConfigParser()
        cfg.readfp(open('scrapy.properties'))
        self.conf = {k: loads(v) for k,v in cfg.items('autodiscover')}

    def render_GET(self, txrequest):
        return self.conf


class ListJobs(WsResource):

    def render_GET(self, txrequest):
        project = txrequest.args['project'][0]
        spiders = self.root.launcher.processes.values()
        
        #~ running = [{"id": s.job, "spider": s.spider} for s in spiders if s.project == project]
        running = [{"id": s.job, "spider": "%s_%s" % (s.spider, "".join(a.split("://")[1] for a in s.args if "start_urls" in a).replace(".","_"))} for s in spiders if s.project == project]
        queue = self.root.poller.queues[project]
        pending = [{"id": x["_job"], "spider": x["name"]} for x in queue.list()]
        finished = [{"id": s.job, "spider": s.spider,
            "start_time": s.start_time.isoformat(' '),
            "end_time": s.end_time.isoformat(' ')} for s in self.root.launcher.finished
            if s.project == project]
        return {"status":"ok", "pending": pending, "running": running, "finished": finished}
