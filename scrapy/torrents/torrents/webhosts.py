"""
Functions related to scraping webhosts.
"""

import time
import re
from urlparse import urlsplit
from datetime import datetime
from urllib import unquote

from MySQLdb import IntegrityError

from scrapy.selector import HtmlXPathSelector
from scrapy.http.request import Request

from torrents.items import TorrentsItem

from learn.utils import is_base32, is_hex, format_ih
from learn.meta import is_season_episode

from ConfigParser import ConfigParser
import pymongo

def scrap_final(response, stats, db, tbl_src, known_data=None,
                add_candidates=True):
    "Return list of requests or final items"
    return scrap_final_item(response, stats, db, tbl_src, known_data,
                            add_candidates)


#  ************************************************************************
#  *                                                                      *
#  *                            Useful classes                            *
#  *                                                                      *
#  ************************************************************************


class SiteError(Exception):
    def __str__(self):
        return 'Site Error: %s' % Exception.__str__(self)


# How to extract hashes from urls in different domains
class pattern:

    "Extract hashes from urls, depending on the destination table"

    @classmethod
    def extract(cls, table, url):
        if not url.startswith('http://'):
            return url
        f = 'extract_' + table
        if hasattr(cls, f):
            return getattr(cls, f)(url)
        else:
            return url




#  ************************************************************************
#  *                                                                      *
#  *                            Main function                             *
#  *                                                                      *
#  ************************************************************************


def is_ih(url):
    return (len(url) == 40 and is_hex(url)) or (len(url) == 32 and is_base32(url))

#~ def scrap_final_item(response, stats, db, tbl_src, known_data=[],
                     #~ add_candidates=True):
def scrap_final_item(response, stats, db, tbl_src, known_data=None, add_candidates=True):
    """Return a list with the final item(s), and update the database.

    """
    # Common fields we will be using.
    if known_data is None:
        return []
        
    table, source = tbl_src[0]
    
    date = time.strftime('%d-%m-%Y %H:%M:%S', time.gmtime(time.time()))
    
    url_old = response.url
    
    #~ url_new = pattern.extract(table, url_old)  # maybe unquote(url_old) ?
    url_new = url_old
    
    #save data in db
    col_domain = db[table]
    
    
    ih = known_data[2]["torrent:infohash"] if "torrent:infohash" in known_data[2] \
         else known_data[2]["infohash"] if "infohash" in known_data[2] \
         else url_new if is_ih(url_new) else None
         
         
    if ih:
        ih = format_ih(ih)
        data = {"_id":response.meta['url_discovery'], "ih":ih.lower()}
        col_domain.update({"_id":response.meta['url_discovery']}, {"$set":{"ihs.%s"%ih.lower():{"url": response.url, "ls": datetime.now()}}}, upsert = True)
        
        db.torrent.update({"_id":ih.lower()},{"$addToSet":{"disc":response.meta['url_discovery']}},upsert=True)
    else:
        print known_data
        raise Exception("ih lost")
    
    
    # Get info and update all that's necessary
    if 'remove' in response.meta:
        raise SiteError(*response.meta['remove'])

    fname, size, meta_last = known_data
    meta = response.meta['info'].copy() if 'info' in response.meta else {}
    meta.update(meta_last)
   
    blacklist_fname = ["...", "nbsp", "laquo", "raquo", "aacute", "eacute", "iacute", "oacute", "uacute", "ntilde"]
    
    
    
    filename = "" if (fname == 'unknown' or \
                fname.lower().startswith("download.") or \
                fname.lower().startswith("download_direct.") or \
                any(b in fname.lower() for b in blacklist_fname) or \
                fname.lower().replace(".torrent","").isdigit() or \
                is_ih(fname.lower().replace(".torrent",""))) else fname
    
    if len(filename) == 0:
        filename = meta['title'] if 'title' in meta else meta['filedir'] \
                if 'filedir' in meta else meta['filepaths'].split("///")[0] \
                if "filepaths" in meta else "UNKNOWN"
        
    
    
    # kind of unfortunate, we use the word "meta" for something
    # different than what scrapy calls "meta" in "response.meta"
    # (our meta = response.meta['info'] + last field of extract_info())
    
    if fname == 'UNKNOWN':  # hack: we couldn't parse the page, so ignore
        table, source = 'candidates', -1
        return None
        
    
    #~ if source == -1:  # we don't want unknown sources in the logs
        #~ return []
    
    # Add new item with the information we got
    items = [create_item(date, filename, size, source, url_new, meta, tbl_src[1])]
    
    return items
 



#  ************************************************************************
#  *                                                                      *
#  *                           Helper functions                           *
#  *                                                                      *
#  ************************************************************************


def create_item(date, filename, size, source, url, meta_dirty, tbl_src):
    "Return an item with all necessary data for the logfile"

    # Remove bad chars from any text, in case one slipped thru.
    meta = {k.replace("image","thumbnail"):re.sub('[\t\n\r\f\v]', ' ', v) for k,v in meta_dirty.iteritems()}

    # This is what we will save in the logs:
    #   identificador - fecha\tfilename\tsize\torigenes\tschema\t
    #   countOrigenes\tschema:meta1=value1\tschema:meta2=value2\t...
    
    
    
    if is_ih(url):
        srcs = {7: format_ih(url)}
    else:
        server = '.'.join(urlsplit(url).netloc.split('.')[-2:])
        source = tbl_src.get(server, ("null",-1))[1]
        if source == -1:
            source = create_new_origin(server)[1]
        #~ print source, url
        srcs = {source: url}
    
    ih_meta = format_ih(meta['infohash'] if 'infohash' in meta else meta['torrent:infohash'] if 'torrent:infohash' in meta else None)
    if not 7 in srcs and ('infohash' in meta or 'torrent:infohash' in meta):
        srcs[7] = ih_meta
        
    if ih_meta and not ih_meta in srcs[7]:
        print ih_meta
        print srcs[7]
        raise Exception("lio")
        
    
    
    origins = "||".join("%d:%s" % (source, url) for source, url in srcs.items())
    
    # All fields except for "meta"
    item = TorrentsItem()
    item['fid'] = '88 - %s' % date  # we don't use the "88", so don't care
    item['filename'] = re.sub('[\t\n\r\f\v]', ' ', filename)
    item['size'] = '%d' % size
    item['origins'] = origins
    item['schema'] = 'torrent' if 'schema' not in meta else meta['schema']
    
    se = is_season_episode(filename)
    if se and not any(":season" in k for k in meta):
        meta['torrent:season'] = se['s']
        meta['torrent:episode'] = se['e']
        if not 'torrent:category' in meta:
            meta['torrent:category'] = "TV"
        else:
            if not "TV" in meta['torrent:category']:
                meta['torrent:category'] = ",".join(meta['torrent:category'].split(",") + ["TV"])
        
        if not 'torrent:tags' in meta:
            meta['torrent:tags'] = "series"
        else:
            if not "series" in meta['torrent:tags']:
                meta['torrent:tags'] = ",".join(meta['torrent:tags'].split(",") + ["series"])
        
    
    # Prepend "<schema>:" to all tags except for the "special:" ones.
    # Do nothing if a tag has already a schema.
    special_tags = {'image_nice', 'entity_nice_series', 'entity_nice_movie',
                    'entity_nice_documentary', 'entity_nice_book',
                    'entity_nice_song', 'entity_image', 'mime_type'}
    
    torrent_tags = {'filedir', 'trackers', 'createdBy', 'filepaths', 
                    'filesizes', 'creationdate', 'creation_date', 'infohash', 'name', 
                    'category', 'tags'}
    
    keys_special = set(meta) & special_tags  # intersection
    keys_torrents = set(meta) & torrent_tags  # intersection
    keys_w_schema = {k for k in set(meta) if ':' in k}  # keys *with* schema
    keys_wo_schema = set(meta) - keys_special - keys_torrents - keys_w_schema - {'schema'}
    item['meta'] = '\t'.join(
        ['%s=%s' % (k, meta[k]) for k in keys_w_schema] + \
        ['%s:%s=%s' % (item['schema'], k, meta[k]) for k in keys_wo_schema] + \
        ['special:%s=%s' % (k, meta[k]) for k in keys_special]
        
    try:
        item['meta'] += ['torrent:%s=%s' % (k, meta[k]) for k in keys_torrents])
    except UnicodeDecodeError:
        try:
            item['meta'] += ['torrent:%s=%s' % (k, meta[k].encode("utf-8") for k in keys_torrents])
        except:
            item['meta'] += ['torrent:%s=%s' % (k, meta[k].decode("utf-8") for k in keys_torrents])
        

    

    return item



def create_error_item(date, source, url, errorType):
    "Return an appropriate item for the log"

  # It must look like:
  # *98 - 27-01-2012 10:33:42    13:http://www.youtube.com/.....    G5
  
    item = TorrentsItem()
    item['fid'] = '*88 - ' + date
    item['origins'] = '%d:%s' % (source, url)
    item['errorType'] = errorType

    return item


def create_new_origin(domain):
    """create new source"""
    
    cfg = ConfigParser()
    cfg.readfp(open('scrapy.properties'))
    db_sources = pymongo.MongoClient(cfg.get('mongo','host_sources'),int(cfg.get('mongo','port_sources'))).foofind
    #maybe exists?
    id_src = db_sources.source.find_one({"d":domain}, {"_id":1})
    if id_src:
        return domain, id_src['_id'] 
    
            
    
    id_src = db_sources.source.find({},{"_id":1}).sort([("_id",-1)])[0]['_id'] + 1 
    
    db_sources.source.save({"_id" : id_src,"crbl" : 0,"d" : domain, "g" : ["p","t"],"tb" : domain.split(".")[0] ,"url_lastparts_indexed" : 0})
    return domain, id_src


#  ************************************************************************
#  *                                                                      *
#  *                       Main extracting function                       *
#  *                                                                      *
#  ************************************************************************

