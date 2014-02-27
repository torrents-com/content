#!/usr/bin/env python

# From
# http://twistedmatrix.com/documents/current/web/howto/using-twistedweb.html
#
# And modified to accept a maximum size following the example at:
# http://stackoverflow.com/questions/6491932/need-help-writing-a-twisted-proxy

# JBC. March 2012.

"""\
Be a nice http proxy server. To use with scrapy spiders.
"""

import sys, logging
from optparse import OptionParser
from urlparse import urlparse

from twisted.web import proxy, http
from twisted.internet import reactor


# So a proxy.Proxy really has the meat deep inside, in ProxyClient:
#
# Proxy.requestFactory = ProxyRequest
#   ProxyRequest.protocols = {'http': ProxyClientFactory}
#     ProxyClientFactory.protocol = ProxyClient
#       ProxyClient has rawDataReceived (from HTTPClient) and handleResponseEnd
#
# All this is in twisted/web/proxy.py
#
# Our main interest is the rawDataReceived() function it inherits from
# HTTPClient, at twisted/web/http.py

class MyProxyClient(proxy.ProxyClient):
    MAX_BYTES = {'html': 10000000,
                 'xml': 10000000}
    # would be nice if configurable, but wtf

    def __init__(self, *args, **kwargs):
        proxy.ProxyClient.__init__(self, *args, **kwargs)
        self.buffer = ''
        self.extension = urlparse(self.rest).path.split('.')[-1]

    def rawDataReceived(self, data):
        self.buffer += data
        max_bytes = self.MAX_BYTES.get(self.extension, 500000)
        if len(self.buffer) > max_bytes:
            logging.warning('Total data (%d) exceeded maximum size (%d)' % \
                                (len(self.buffer), max_bytes))
            self.handleResponseEnd()

    def handleResponseEnd(self):
        if not self._finished:
            self.father.responseHeaders.setRawHeaders('content-length',
                                                      [len(self.buffer)])
            self.father.write(self.buffer)
        proxy.ProxyClient.handleResponseEnd(self)



class ProxyFactory(http.HTTPFactory):

    class MyProxy(http.HTTPChannel):

        class ProxyRequest(proxy.ProxyRequest):

            class MyProxyClientFactory(proxy.ProxyClientFactory):
                protocol = MyProxyClient

            protocols = {'http': MyProxyClientFactory}

            def process(self):
                try:
                    proxy.ProxyRequest.process(self)
                except KeyError:
                    logging.warning('Error processing request - skipped')
            # TODO: trap other errors, like using Request.notifyFinish
            # This may come handy:
            # http://twistedmatrix.com/documents/current/web/howto/web-in-60/interrupted.html

        requestFactory = ProxyRequest

    def buildProtocol(self, addr):
        logging.info('New connection from: %s' % addr)
        return self.MyProxy()



def main():
    parser = OptionParser(usage='%prog [--port <num>] [--verbose]',
                          description=__doc__)
    parser.add_option('-p', '--port', type='int', default=8080,
                      help='listening port')
    parser.add_option('-l', '--logfile', default='-',
                      help='name of logging file ("-" for stderr)')
    parser.add_option('-v', '--verbose', action='store_true')
    opts, rest = parser.parse_args()

    kwargs = {'format': '%(asctime)s %(levelname)s %(message)s',
              'level': logging.DEBUG if opts.verbose else logging.WARN}
    if opts.logfile != '-':
        kwargs['filename'] = opts.logfile
    logging.basicConfig(**kwargs)

    reactor.listenTCP(opts.port, ProxyFactory())
    reactor.run()



if __name__ == '__main__':
    main()
