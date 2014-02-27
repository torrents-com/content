#!/usr/bin/env python

"""
Utilities for the smon-* commands.
"""

# See all the services available in the standard scrapy server at:
# http://doc.scrapy.org/en/latest/topics/scrapyd.html
#
# Also, see the extra ones we define at scrapyd.conf . They are:
# info, config, data, getSettings


import os

from urllib2 import urlopen, URLError
from json import loads, dumps

import smtplib
from email.mime.text import MIMEText



def webread(subdir, data=None):
    """Returns data requested to autodiscover."""
    config = {}
    execfile(os.path.join(os.path.dirname(__file__),"tools.conf"), config)
    urlbase = os.environ['SMON_URLBASE'] if 'SMON_URLBASE' in os.environ \
        else config['default_server']
    
    return loads(urlopen(urlbase + subdir, data=data, timeout=15).read())


def get_spiders():
    return webread('listspiders.json?project=default')['spiders']


def get_jobs():
    return webread('listjobs.json?project=default')['running']


def add(spider, spider_opts=[]):
    """Schedule running a spider."""
    #~ used_ports = map(int, webread('config')['subdirs'])
    #~ port = max([9000] + used_ports) + 1  # use next port available
    params = spider_opts + ['project=default', 'spider=%s' % spider]
                        
    
    response = webread('schedule.json', data='&'.join(params))

    if response['status'] != 'ok':
        raise RuntimeError(response)

    return response['jobid']  # which you probably will ignore anyway


def rm(jid_or_spider):
    """Stop a scrapy spider, either a specific job or all from same spider."""
    jids = [x['id'] for x in get_jobs() \
                if x['spider'] == jid_or_spider or x['id'] == jid_or_spider]
    if not jids:
        raise RuntimeError('Error: unknown job or spider: %s' % jid_or_spider)

    for jid in jids:
        params = 'project=default&job=%s' % jid
        response = webread('cancel.json', data=params)
        if response['status'] != 'ok':
            raise RuntimeError('Error removing job %s\n' \
                                   'Server response:\n %s' % (jid, response))


def email(txt, subject='Greetings from scrapy', receivers='jordi@mp2p.net'):
    """Send an email with the specified text."""
    msg = MIMEText(txt)
    msg['From'] = 'scrapy checker <name@mail.net>'
    msg['To'] = receivers
    msg['Subject'] = subject

    s = smtplib.SMTP('smtp.server.com')
    s.sendmail('name@mail.com', receivers.split(','), msg.as_string())
    s.quit()


def get_json_rpc(key, port):  # JSON-RPC done by hand
    url = 'http://localhost:%d/stats' % port
    data = dumps({'method': 'get_value', 'params': [key], 'id': 1})
    return loads(urlopen(url, data=data, timeout=15).read())['result']
    # Use like: get_json_rpc('lines', 9001)
    ## Not used for the moment...
