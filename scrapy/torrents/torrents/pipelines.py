# Item Pipeline: what we want to do with the items.
#
# JBC. May 2012.

# The pipeline has to be added to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging.handlers
import socket

from ConfigParser import ConfigParser


class TorrentsPipeline(object):
    def open_spider(self, spider):
        # Create a logger following Reset's indications. Thanks Reset!
        cfg = ConfigParser()
        cfg.readfp(open('scrapy.properties'))
        args = ''
        
        if spider.name == "discover":
            return
        
        if spider.fresh:
            args += ':fresh'
        if hasattr(spider, 'group'):
            args += ':group=' + spider.group
        
        try:
            fname = '%s_%s%s.%s.log' % (spider.name, spider.site, args, socket.gethostname())
        except KeyError:
            fname = '%s%s.%s.log' % (spider.name, args, socket.gethostname())
        
        handler = logging.handlers.TimedRotatingFileHandler(
            filename='%s/%s' % (cfg.get('paths', 'logdir'), fname),
            when='M', interval=60)  # log every 60 minutes
        handler.suffix = '%Y-%m-%d-%H-%M'

        
        self.logger = logging.getLogger(spider.name)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def process_item(self, item, spider):
        """Store the item into the log file."""
        
        if spider.name == "discover":
            return

  # For the error formats, it must look like:
  #   *98 - 27-01-2012 10:33:42    13:http://www.youtube.com/.....    G5

        if not item['fid'].startswith('*'):  # normal case
            fields = 'fid filename size origins schema meta'.split()
            spider.crawler.stats.inc_value('lines')
        else:  # entry to remove
            fields = 'fid origins errorType'.split()
            spider.crawler.stats.inc_value(item['errorType'])
        
        self.logger.info('\t'.join(item[x] for x in fields).encode('utf-8'))

        return item
