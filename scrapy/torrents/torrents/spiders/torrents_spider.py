from ConfigParser import ConfigParser, NoSectionError

import raven, pymongo

from scrapy import log
from scrapy.spider import BaseSpider


class TorrentsSpider(BaseSpider):

    """Base class for spiders. Connects to the database and gets table info."""

    def __init__(self, fresh=False, nosentry=False, *args, **kwargs):
        super(TorrentsSpider, self).__init__(*args, **kwargs)

        try:
            cfg = ConfigParser()
            cfg.readfp(open('scrapy.properties'))
            
            
            self.db = pymongo.MongoClient(cfg.get('mongo','host'),int(cfg.get('mongo','port'))).torrents
            self.db_sources = pymongo.MongoClient(cfg.get('mongo','host_sources'),int(cfg.get('mongo','port_sources'))).foofind

            self.load_srcs_tables()

            self.sentry = raven.Client(cfg.get('sentry', 'dsn')) \
                if not nosentry and 'sentry' in cfg.sections() else None
        except IOError as e:  # file not found
            log.err('Configuration file not found: %s' % 'scrapy.properties')
            raise
        except NoSectionError as e:  # section missing
            log.err(str(e))
            raise
    
        self.fresh = fresh  # are we only picking fresh links?
    
        self.first_unknown_site = True  # used to warn at the 1st unknown site
        
    
    
    def load_srcs_tables(self):
        # Get and save the correspondence of servers->table
        self.tbl_src = {origin['d']:(origin['d'].replace(".", "_"), origin['_id']) for origin in self.db_sources.source.find({},{"d":1, "crbl":1}) if not "crbl" in origin or origin['crbl'] == 0}
    
    def get_table_source(self, site):
        "Return the (table, source) that corresponds to a given site"

        server = '.'.join(site.split('.')[-2:])  # last two parts
        table, src = self.tbl_src.get(server, ('candidates',-1))
        if src > -1:
            return table, src
        else:
            return self.create_new_origin(server)
        
    def create_new_origin(self, domain):
        id_src = self.db_sources.source.find({},{"_id":1}).sort([("_id",-1)])[0]['_id'] + 1 
        self.db_sources.source.save({"_id" : id_src,"crbl" : 0,"d" : domain, "g" : ["p","t"],"tb" : domain.split(".")[0] ,"url_lastparts_indexed" : 0})
        self.load_srcs_tables()
        return domain, id_src


    def parse(self, response):
        "Return parsed items/requests from response, log errors to sentry"
        try:
            for item in self.parse_sentry(response):
                yield item
        except Exception:
            if self.sentry is not None:
                self.sentry.captureException()
                return
            else:
                raise
        # If any spider doesn't want to use sentry, it only needs to
        # overwrite the parse() member function, instead of defining
        # parse_sentry()

