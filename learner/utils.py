#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii, bson, threading, sys, urllib, urllib2, gzip, StringIO, string
import socket, time, httplib, re
from base64 import b64decode, b32decode
from meta import tags
from bencode import bdecode, bencode
from hashlib import sha1

config = {}
execfile("learn.conf", config)
secret = config['secret_url']

def strip_tags (text):
  """
    Strip HTML tags from any string and transfrom special entities
  """
 
  # apply rules in given order!
  rules = [
    { r'>\s+' : u'>'},                  # remove spaces after a tag opens or closes
    { r'\s+' : u' '},                   # replace consecutive spaces
    { r'\s*<br\s*/?>\s*' : u'\n'},      # newline after a <br>
    { r'</(div)\s*>\s*' : u'\n'},       # newline after </p> and </div> and <h1/>...
    { r'</(p|h\d)\s*>\s*' : u'\n\n'},   # newline after </p> and </div> and <h1/>...
    { r'<head>.*<\s*(/head|body)[^>]*>' : u'' },     # remove <head> to </head>
    { r'<a\s+href="([^"]+)"[^>]*>.*</a>' : r'\1' },  # show links instead of texts
    { r'[ \t]*<[^<]*?/?>' : u'' },            # remove remaining tags
    { r'^\s+' : u'' }                   # remove spaces at the beginning
  ]
 
  for rule in rules:
    for (k,v) in rule.items():
      regex = re.compile (k)
      text  = regex.sub (v, text)
 
  # replace special strings
  special = {
    '&nbsp;' : ' ', '&amp;' : '&', '&quot;' : '"',
    '&lt;'   : '<', '&gt;'  : '>'
  }
 
  for (k,v) in special.items():
    text = text.replace (k, v)
 
  return text
    

def url2tracker_id(url_tracker):
    try:
        return str(binascii.crc32(url_tracker))
    except UnicodeEncodeError:
        return "-1"
    except:
        print url_tracker
        raise
    
def bin2hex(_bin):
    return binascii.hexlify(_bin).lower()
        
def url2mid(b64id):
    return bin2mid(url2bin(b64id))
    
def bin2mid(binary):
    #~ return bson.objectid.ObjectId(binary.encode("hex"))
    return binary.encode("hex")
    
def url2bin(url):
    return ''.join(chr(c) for c in [ord(a) ^ ord(b) for a,b in zip(b32decode(url+"====", True), secret)])[::-1]


def make_async(function):
    '''
    Definición
    ==========

    El decorador C{make_async} sirve para realizar funciones de lanzar y olvidar, es decir, no se espera que
    la función devuelva algún valor. En todo caso, se dispone de la variable C{self} si se aplica a un método
    de una clase, con lo que se pueden devolver valores a través de la misma, pero hay que vigilar de usar entonces
    métodos I{thread safe} o bien herramientas de sincronización de procesos y bloqueos.
    '''
    def inner(*args, **kwargs):
        '''
        Esta es la función que retornamos
        '''
        t = threading.Thread(target=function, args=args, kwargs=kwargs)
        t.start()
        
    return inner
    
class RedirctHandler(urllib2.HTTPRedirectHandler):
    """docstring for RedirctHandler"""
    def http_error_302(self, req, fp, code, msg, headers):
        #~ print headers
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302
        
def download_url(url, force = False):
    
    if not "?" in url:
        url = urllib.quote_plus(url.encode("utf-8"), ":/")
    #~ print url
    if len(url) > 140 and not force:
        return ""
    req_headers = { 'User-Agent' : 'torrents', 
                       'Referer' : url, 
                       'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
                       'Accept-Encoding' : 'gzip, deflate' }
                       
                       


    #~ print url
    cookieprocessor = urllib2.HTTPCookieProcessor()
    request = urllib2.Request(url, headers = req_headers)
    opener = urllib2.build_opener(RedirctHandler, cookieprocessor)
    urllib2.install_opener(opener)

    loops = 0
    r = None
    while loops < 5:
        try:
            loops += 1
            r = urllib2.urlopen(request, timeout = 30)
            #~ print r.headers
            break
        except (socket.timeout, urllib2.URLError, httplib.BadStatusLine, socket.error), err:
            #~ print err.code
            time.sleep(loops *2)
        except urllib2.HTTPError, err:
            #~ print err.code
            if err.code in [404, 410, 403] :
               return "" 
            else:
               time.sleep(loops *2)
        except ValueError:
            
            return ""
        except:
            raise
            return ""
    #~ print r
    if r is None:
        return ""
    
    
    #~ print r.headers['content-type']
    if not force:
        if not 'content-type' in r.headers:
            return ""
        
        if not "text/html" in r.headers['content-type']:
            return ""
    
    if 'content-encoding' in r.headers and r.headers['content-encoding'] == 'gzip':
        try:
            html = gzip.GzipFile(fileobj = StringIO.StringIO(r.read())).read()
        except IOError:
            return None
    else:
        html = r.read()
    
    #~ print "url %s [%d]"%(url, len(html))
    
    return html
    
def save_tmp(url, domain):
    
    
    #~ print url
    f_temp = "temp/temp_%s"%domain
    f = open(f_temp,"w")
    f.write(download_url(clean_url_img(url, domain), force = True))
    f.close()
    return f_temp
    
def clean_url_img(url, domain):
    if not url.startswith("http://"):
        if url.startswith("//"):
            url = "http:" + url
        if url.startswith("/"):
            url = domain + "/" + url
    return url
    
def u(txt, raise_exception = False):
    ''' 
    Parse any basestring (ascii str, encoded str, or unicode) to unicode 
    '''
    if isinstance(txt, unicode):
        return txt

    elif isinstance(txt, basestring):
        try:
            cdt = chardet.detect(txt)["encoding"]
            if not cdt is None:
                return unicode(txt, cdt)
            else:
                return txt

        except:
            if raise_exception:
                raise

            else:
                return unicode("")

    return unicode(txt)
    
def element_2_str(element, depth = 0):
    #~ return " ".join([s for s in element if isinstance(s,basestring) else '' if s.text is None else s.text ])
    s = ""
    #~ debug = []
    for e in element:
        if isinstance(e,basestring):
            #~ debug.append(e)
            s += e
            #~ print "\t"*depth, e
        else:
            #~ print "\t"*depth, e
            #~ print "\t"*depth, e.tag
            #~ print "\t"*depth, e.text
            if not e.text is None:
                #~ debug.append(e.text)
                s += e.text
            else:
                for a in e:
                    s += element_2_str(a, depth + 1)
            #~ print "++++++++++"
            #~ print s
    #~ print "@@@@@@@@@@@@@@@"
    #~ print debug
    #~ print "@@@@@@@@@@@@@@@"
    return s
    
def ugly_id(_id):
    #es feo si tiene más de 3 números en el id
    
    return sum([1 if c.isdigit() or c == "_" else 0 for c in _id]) > 3 
    
    
def get_xpath_from_soup_object(so):

    c = 1
    for s in so.find_previous_siblings():
        #~ print "pp\t", s.name
        if s.name == so.name:
            c += 1
    
    if c > 1 or so.name in [s.name for s in so.find_next_siblings()]:
        xpath = "/a[%d]/@stext()"%c
    else:
        xpath = "/a/@stext()"
    
    if len(list(so.children)) > 1:
        raise Exception("No parece unico")
    
    child = list(so.children)[0]
    
    xpath = xpath.replace("@s","" if isinstance(child,basestring) else child.name +"/")
    
    for parent in so.parents:
        if not parent is None:
            #~ print parent.name
            _xp = "/" + parent.name
            if "id" in parent.attrs and not ugly_id(parent.attrs['id']):
                xpath = '/%s[@id="%s"]'%(_xp, parent.attrs['id']) + xpath
                return xpath
                
            c = 1
            for s in parent.find_previous_siblings():
                #~ print "p\t", s.name
                if s.name == parent.name:
                    c += 1
                    
        
            if c > 1 or [p.name for p in parent.find_next_siblings()]:
                _xp += "[%d]" % c
            xpath = _xp + xpath
            if parent.name == "html":
                break

    return xpath


  
def _encode(v):
    if v < 26:
        return chr(ord('a') + v)
    else:
        return chr(ord('2') + (v - 26))
  
BASE32_ALPHABET = "".join([_encode(i) for i in range(0x20)])
# 'abcdefghijklmnopqrstuvwxyz234567'
  
def is_base32(s):
    return all(c in BASE32_ALPHABET for c in s.lower())
    
def is_hex(s):
    return all(c in string.hexdigits for c in s)
    
def base32_2_hex(s):
    return binascii.hexlify(b32decode(s)).upper()

def format_ih(s):
    if is_base32(s):
        return base32_2_hex(s).upper()
    else:
        return s.upper()

def torrent_info(content_raw):
    "Read info from torrent (in raw string form) and return it"

    # See torrent encoding at http://en.wikipedia.org/wiki/Torrent_file

    # Extract info from torrent
    torrent = bdecode(content_raw)
    t_info = torrent['info']  # so we write less

    # Trackers. Use extension BEP-0012 for multiple trackers if possible
    info = {}
    if 'announce-list' in torrent:
        info['trackers'] = ' '.join(' '.join(x) if type(x) == list else x \
                                        for x in torrent['announce-list'])
        # The "type(x) == list" part is due to common malformed torrents
    else:
        info['trackers'] = torrent['announce']

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
        info['filedir'] = get8(t_info, 'name').decode('utf-8')
        paths = ['/'.join(map(str, get8(f, 'path'))) for f in t_info['files']]
        # The "map(str, ...)" is due to torrents with numeric directories (!)
        sizes = [f['length'] for f in t_info['files']]
        info['filepaths'] = '///'.join(paths).decode('utf-8')
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
        comment = get8(torrent, 'comment').decode('utf-8')
        for k,v in [('\r\n', '<br />'), ('\n', '<br />'), ('\t', ' ')]:
            comment = comment.replace(k, v)
        info['comment'] = comment

    # Add creation date if present
    if 'creation date' in torrent:
        secs = torrent['creation date']
        info['creation_date'] = time.strftime('%d-%m-%Y %H:%M:%S',
                                              time.localtime(secs))

    # Add nodes (list of host:port) if present (see BEP-0005)
    if 'nodes' in torrent:
        info['nodes'] = ' '.join('%s:%d' % tuple(hp) for hp in torrent['nodes'])

    return info

def is_same_torrent(extract, info):
    """ check metadata match """
    try:
        if ("size" in extract and ((float(extract['size']) / info['size'])>1.01 or (float(extract['size']) / info['size'])<0.99)) or   \
            ("infohash" in extract and extract['infohash'].lower() != info['infohash'].lower()):
            return False
        else:
            #the words of title must be in the torrent
            if 'title' in extract:
                words = [w for w in re.findall(r"[\w']+", extract['title']) if len(w)>3]
                words_torrent = list(set(w for w in re.findall(r"[\w']+", "%s %s" % (info['filedir'] if 'filedir' in info else '', info['filepaths'])) if len(w)>3))
                
                if  sum(w in words_torrent for w in words) < len(words) / 2:
                    return False
                
        return True
    except:
        print "*"*22
        print extract
        print info
        raise
