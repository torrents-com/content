"""
Scrap information from files.
"""

import time
import urllib2, chardet
from urllib import unquote
from gzip import GzipFile
from StringIO import StringIO
from tempfile import NamedTemporaryFile
from urlparse import urlsplit, parse_qs
import base64, binascii
from hashlib import sha1

from bencode import bdecode, bencode

from scrapy.http.request import Request
from scrapy.http.response.html import HtmlResponse
from scrapy import log

from HTMLParser import HTMLParser



#  ************************************************************************
#  *                                                                      *
#  *                         Magnets and torrents                         *
#  *                                                                      *
#  ************************************************************************


def torrent_info(content_raw):
    "Read info from torrent (in raw string form) and return it"

    # See torrent encoding at http://en.wikipedia.org/wiki/Torrent_file

    # Extract info from torrent
    torrent = bdecode(content_raw)
    if not 'info' in torrent:
        return None
    t_info = torrent['info']  # so we write less

    # Trackers. Use extension BEP-0012 for multiple trackers if possible
    info = {}
    if 'announce-list' in torrent:
        info['trackers'] = ' '.join(' '.join(x) if type(x) == list else x \
                                        for x in torrent['announce-list'])
        # The "type(x) == list" part is due to common malformed torrents
    else:
        if 'announce' in torrent:
            info['trackers'] = torrent['announce']  
        else:
            #nodes - dht
            pass

    # Extra step due to some bad torrents that have the zero-width-space char
    info['trackers'] = info['trackers'].replace('\xe2\x80\x8b', '')
    # We could also do: x = x.decode('utf-8').encode('ascii', 'ignore')

    # Auxiliary function, to use with torrents with path.utf-8 and so on
    def get8(d, key):
        "Get value of dict d for key, with '.utf-8' appended if exists"
        key8 = key + '.utf-8'
        return d[key8] if key8 in d else d[key]

    # Fill dirs, paths and sizes, for single- and multiple-file torrents
    if 'files' in t_info:  # multiple files
        try:
            info['filedir'] = get8(t_info, 'name').decode('utf-8')
        except UnicodeDecodeError:
            pass
        paths = ['/'.join(map(str, get8(f, 'path'))) for f in t_info['files']]
        # The "map(str, ...)" is due to torrents with numeric directories (!)
        sizes = [f['length'] for f in t_info['files']]
        try:
            info['filepaths'] = '///'.join(paths).decode('utf-8')
        except UnicodeDecodeError:
            info['filepaths'] = '///'.join(paths)
        info['filesizes'] = ' '.join(map(str, sizes))
        info['size'] = sum(sizes)
    else:                  # single file
        info['filepaths'] = get8(t_info, 'name').decode('utf-8')
        info['filesizes'] = str(t_info['length'])
        info['size'] = t_info['length']

    # Compute infohash (useful for magnets among other things)
    info['infohash'] = binascii.hexlify(sha1(bencode(t_info)).digest())

    # Add comment if present
    if 'comment' in torrent:
        try:
            comment = get8(torrent, 'comment').decode('utf-8')
        except UnicodeDecodeError:
            comment = get8(torrent, 'comment')
            
        for k,v in [('\r\n', '<br />'), ('\n', '<br />'), ('\t', ' ')]:
            comment = comment.replace(k, v)
        info['comment'] = comment
        
            

    # Add creation date if present
    if 'creation date' in torrent:
        secs = torrent['creation date']
        try:
            info['creation_date'] = time.strftime('%d-%m-%Y %H:%M:%S',
                                              time.localtime(secs))
        except:
            info['creation_date'] = "1970-01-01 00:00:00"

    # Add nodes (list of host:port) if present (see BEP-0005)
    if 'nodes' in torrent:
        info['nodes'] = ' '.join('%s:%d' % tuple(hp) for hp in torrent['nodes'])

    return info


def test_torrent_info(data_or_url_or_fname=None):
    "Test that the function torrent_info() works well"

    import os

    if not data_or_url_or_fname:
        url = 'http://torra.ws/torrent/' \
            '640FE84C613C17F663551D218689A64E8AEBEABE.torrent'
        data = urllib2.urlopen(url).read()
    elif data_or_url_or_fname.startswith('http'):
        data = urllib2.urlopen(data_or_url_or_fname).read()
    elif os.path.exists(data_or_url_or_fname):
        data = open(data_or_url_or_fname).read()
    else:
        data = data_test

    return torrent_info(data)



def magnet2resp(magnet_u, url_discovery='unknown', info={}, webcache=True,
                allow_missing=True):
    "Return a Response and known_data with all the info from the magnet link"

    for x,y in [('&amp;', '&'), ('&lt;', '<'), ('&gt;' , '>')]:
        magnet_u = magnet_u.replace(x, y)
    
    parts = parse_qs(magnet_u[len('magnet:?'):])  # extract the sections
    try:
        if "&" in parts['dn'][0]:
            magnet = HTMLParser().unescape(unquote(magnet_u)).encode('utf-8')
        else:
            magnet = magnet_u.encode('utf-8')  # parse_q doesn't work with unicodes
            magnet = unquote(magnet)
        
    except:
        print "err"
        magnet = magnet_u.encode('utf-8')  # parse_q doesn't work with unicodes
        magnet = unquote(magnet)
        
    
    
    if not magnet.startswith('magnet:?'):
        raise RuntimeError('Does not look like a magnet link: %s' % magnet)
    parts = parse_qs(magnet[len('magnet:?'):])  # extract the sections
    

    xt = parts['xt'][0]
    if not xt.startswith('urn:btih:'):
        raise RuntimeError('Magnet link in unexpected format: %s' % xt)

    # urn:btih -> urn:sha1
    btih = xt.split(':')[-1].upper()
    try:
        bth_32 = base64.b32encode(base64.b16decode(btih))
        bth_16 = btih
    except TypeError:
        # backwards compatibility with clients that use a Base32 hash
        bth_32 = btih
        bth_16 = base64.b16encode(base64.b32decode(btih))

    # Get extra info from torcache
    if webcache:
        for cache_site in ['torcache.net', 'zoink.it']:
            # We could also use torra.ws, but the result is not gzipped
            try:
                url = 'http://%s/torrent/%s.torrent' % (cache_site, bth_16)
                data = StringIO(urllib2.urlopen(url, timeout=1).read())
                info_webcache = torrent_info(GzipFile(fileobj=data).read())
                info_webcache.pop('comment', None)  # it's useless
                log.msg('Got extra info from %s!' % cache_site)
                break
            except Exception as e:  # TODO: be less inclusive
                log.msg('Error when asking %s: %s' % (cache_site, e),
                        log.WARNING)
        else:  # none of the cache sites worked
            info_webcache = {}
    else:
        info_webcache = {}

    # Find its name
    if 'dn' in parts:
        #~ cdt = chardet.detect(parts['dn'][0])["encoding"]
        fname = parts['dn'][0].decode('utf-8')
    elif 'filedir' in info_webcache:
        fname = info_webcache['filedir']
    elif 'filepaths' in info_webcache:
        fname = info_webcache['filepaths']
    else:  # buuuh, a magnet with no name!
        message = 'Magnet link has no name ("dn"): %s' % magnet
        if allow_missing:
            log.msg(message, log.WARNING)
            fname = ''
        else:
            raise RuntimeError(message)

    # Get its size if possible
    if 'size' in info_webcache:
        size = info_webcache.pop('size')
    else:
        size = 0  # we don't know its size

    # Get all the trackers that make sense
    trackers = set()
    if 'trackers' in info_webcache:
        trackers |= set(info_webcache.pop('trackers').split())
    if 'tr' in parts:
        trackers |= set(parts['tr'])
    if not trackers:  # no trackers? what kind of a magnet is that, buddy?
        message = 'Magnet link has no trackers ("tr"): %s' % magnet
        if allow_missing:
            log.msg(message, log.WARNING)
        else:
            raise RuntimeError(message)

    # Store all the information and get ready to return it
    info_local = info.copy()
    info_local.update({'torrent:%s' % k: v for k,v in info_webcache.items()})
    info_local['torrent:trackers'] = ' '.join(trackers)

    known_data = [fname, size, info_local]

    # Hack. We put the BTH as the "url" and it will appear in the log:
    #   7:K7RBZRI5OXRIPBCWVMPSEEH4NJR6PG2V
    # or something like that
    meta = {'url_discovery': url_discovery, 'info': {}, 'url4mysql': magnet}
    fake_response = HtmlResponse(url=bth_32,
                                 request=Request('http://x.y', meta=meta))

    return fake_response, known_data
    # This can be used like this:
    #   fake_response, known_data = magnet2resp(link, response.url)
    #   return scrap_final(fake_response, self.crawler.stats,
    #                      self.db, ('magnet', 7), known_data)


def test_magnet2resp(magnet_test=None, webcache=True):
    "Test that the function magnet2resp() works well"

    if magnet_test:
        magnet = magnet_test
    else:
        magnet = 'magnet:?xt=urn:btih:b59346e74d690eb9cd3982dead8e88ad4ddc238c&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&tr=udp%3A%2F%2Ftracker.publicbt.com%3A80&tr=udp%3A%2F%2Ftracker.istole.it%3A80&tr=udp%3A%2F%2Ftracker.ccc.de%3A80&dn=Copia_este_libro_%28David_Bravo_Bueno%29%28Castellano%29%28Gallaecio%29.torrent&xl=2757754'

    return magnet2resp(magnet, webcache=webcache)


