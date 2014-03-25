#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, pymongo, os, operator
from xpath import XPath
from utils import u, download_url
from meta import *
from urlparse import urlparse
import sys

from HTMLParser import HTMLParser

from urlparse import urlsplit
from pprint import pprint

import urllib

class MetaExtractor(object):
    def __init__(self, url, domain = None, db_conn = None, debug = False, force = False):
        self.url = url
        
        self.debug = debug
        
        if domain:
            self.domain = domain
            self.ok = True
            self.len_url = len(url.split("/"))
        else:
            
            if db_conn is None:
                config = {}
                execfile(os.path.join(os.path.dirname(__file__), "learn.conf"), config)
                db_conn = pymongo.MongoClient(config['database_host'],config['database_port'])

            self.domain = db_conn.torrents.domain.find_one({"_id": urlparse( self.url ).netloc })
            if self.domain is None:
                self.domain = db_conn.torrents.domain.find_one({"_id": urlparse( self.url ).netloc.replace("www.","") })
            if force:
                self.ok = True
            else:
                
                if self.domain:
                    #~ print "OK", self.domain['ok'], self.domain['_id'] 
                    self.ok = self.domain['ok'] if 'ok' in self.domain else False
                else:
                    self.ok = False
            
                    
            if not self.ok:
                return
            
            
            
            self.len_url = len(self.domain['tp'][0].split("/"))
            
    def get_links(self):
        
        if not self.ok or self.len_url != len(self.url.split("/")):
            return None
        
        xpath = XPath(self.url)
        if xpath is None:
            return None
        
        if not 'links' in self.domain['md']:
            return None
        
        rt = []
        for xp in self.domain['md']['links']['all']:
            link = xpath.extract(xp, extract_href = True)
            if link:
                rt.append(link)
                
        return rt
        
        
    def extract(self, verbose = False):
        
        if  not self.ok or self.len_url != len(self.url.split("/")):
            #~ print self.url, self.ok
            #~ print "None(1)"
            return None
        
        xpath = XPath(self.url)
        if xpath is None:
            #~ print "None(2)"
            return None
            
        mds = {}
        data = {}
        
        lost_safety = 0
        
        for md in self.domain['md']:
            if verbose:
                print "%s:"%md
            if "@url" in self.domain['md'][md]:
                pos = self.domain['md'][md].split("[")[1].split("]")[0]
                #~ print url.split("/")[int(pos)]
                #~ html += u"<br/>\n<b>%s:</b> %s" % (u(md), u(url.split("/")[int(pos)]))
                ex = self.url.split("/")[int(pos)]
                if is_valid_meta(ex, md):
                    data[md] =  ex
                
            else:
                extract = ""
                safety = False
                if "candidate" in self.domain['md'][md]:
                    extract = xpath.extract(self.domain['md'][md]['candidate'], True)
                    
                    #~ if md == "image" and extract != "":
                        #~ print "IMAGE", self.domain['md'][md]['candidate'], extract
                    #~ print md, extract, self.domain['md'][md]['candidate']
                    
                    safety = True
                extracts = []
                if extract == "":
                    #Si lo ha encontrado en muchos sitios no es fiable, mejor ignorar
                    #~ print self.domain['md'][md]['all']
                    #~ if len(self.domain['md'][md]['all']) > 8:
                        #~ print "********************"
                        #~ print md
                        #~ print len(self.domain['md'][md]['all'])
                        #~ continue
                    
                    safety = False
                    safety_val = 0
                    #Para que un valor se considere seguro al menos un 50% de las apariciones han tenido que ser ahí
                    safety_val = sum(c for c in self.domain['md'][md]['all'].values() if c > 1) * 0.3
                    
                    xps = self.domain['md'][md]['all']
                    sorted_xp = reversed(sorted(xps.iteritems(), key=operator.itemgetter(1)))
                    
                    for xp_tuple in sorted_xp:
                        xp = xp_tuple[0]
                        try:
                            if "comment" in xp:
                                #para que no se lie con comentarios
                                continue
                            #~ print "\t", xp
                            if "@url" in xp:
                                pos = xp.split("[")[1].split("]")[0]
                                #~ print url.split("/")[int(pos)]
                                #~ html += u"<br/>\n<b>%s:</b> %s" % (u(md), u(url.split("/")[int(pos)]))
                                #~ print self.url
                                
                                ex = self.url.split("/")[int(pos)]
                                if is_valid_meta(ex, md):
                                    data[md] = ex
                                
                                break
                            if self.domain['md'][md]['all'][xp] == 1:
                                #una única aparación es ignorada
                                continue
                            
                            extract = xpath.extract(xp, True)
                            if verbose:
                                print "\t", xp, extract
                            
                            #~ if md == "description":
                                #~ print
                                #~ print
                                #~ print
                                #~ print
                                #~ print md, xp, self.domain['md'][md]['all'][xp], len(extract)
                                #~ if len(extract)<100:
                                    #~ print extract
                            
                            
                            if extract == "":
                                continue
                            
                            #~ print "\t\t", extract
                            #~ print md, extract, xp
                            
                            extracts.append(extract)
                            #fiable si aparece mucho 
                            if self.domain['md'][md]['all'][xp] > safety_val:
                                safety = True
                                break
                        except (UnicodeDecodeError, TypeError):
                            continue
                
                #~ if md == "description":
                    #~ print len(extract), len(extracts), safety, md, is_valid_meta(extract, md)
                    #~ print is_script(extract)
                
                _md = {}
                #~ print md, extracts
                if safety:
                    #~ print md, extracts
                    safety = is_valid_meta(extract, md)
                    if not safety:
                        lost_safety += 1
                        
                    
                    #~ print safety
                    
                    
                    if isinstance(safety, basestring):
                        if not md in ["size", "infohash", "category", "episode", "season"]:
                            md = safety.split("_")[0]
                        else:
                            extract = safety
                        
                        _md[md] = extract
                        safety = True
                
                
                if not safety:
                    ok = False
                    def compare(x, y):
                        return len(y) - len(x)
                    
                    #~ _extract = {}
                    #para la descripción prima la más grande pero para el resto la primera aparación
                    extracts_sorted = sorted(extracts, cmp=compare) if md != "category" else extracts 
                    for ext in extracts_sorted:
                        
                        
                        rt = is_valid_meta(ext, md)
                        if self.debug:
                            print "ITEM:", ext, extracts, rt
                        
                        #~ if md == "category":
                            #~ print "-----"
                            #~ print ext, extracts, rt
                        
                        if rt:
                            if md in _md: 
                                #~ if md != "description":
                                    #~ print "Descartando", ext, md, _md[md]
                                continue
                            ok = True
                            if isinstance(rt, bool):
                                #~ extract = ext
                                _md[md] = ext
                            else:
                                #~ print md, rt, ext
                                if md in ["size", "infohash", "episode", "season"]:
                                    #~ extract = rt
                                    _md[md] = rt
                                    #~ _extract.append(ext)
                                else:
                                    #~ print "...", rt, ext
                                    if rt in ext.lower():
                                        #~ extract = rt
                                        _md[md] = rt
                                        #~ _md.append(md)
                                        #~ _extract.append(ext)
                                    else:
                                        #~ print rt, extract, ext
                                        #~ print "[[", rt
                                        #~ md = rt.split("_")[0]
                                        #~ _md.append(rt.split("_")[0])
                                        #~ print md
                                        
                                        #~ _extract.append(ext)
                                        _md[rt.split("_")[0]] = ext

                    if not ok:
                        if len("".join(extracts)) < 2:
                            continue
                        extract += "\n\t\t[%s]\n"%("\n\t\t * ".join(extracts))
                
                #~ print "\t\t%s"%extract
                if safety and len(_md) == 0:
                    _md[md] = extract
                
                for md, extract in _md.items():
                    if not is_valid_meta(extract, md):
                        
                        #~ print "NOT VALID", extract if len(extract) < 100 else len(extract), md
                        continue
                    
                    
                    if md=="image":
                        if extract != "":
                            mds[md] = {"safety": safety, "data": [extract]}
                            #~ html += u"<br/>\n<b>Imagen%s:</b> <img src='%s'>" % ("(SAFE)" if safety else "", extract)
                    else:
                            
                        if md == "infohash":
                            extract = extract_infohash(extract)
                        if md == "size":
                            #~ print extract
                            z = is_size(extract)
                            #~ print z
                            if z:
                                extract = z
                            
                        
                        if md in mds:
                            mds[md]['data'].append(str(extract).encode("utf-8"))
                        else:
                            try:
                                mds[md] = {"safety": safety, "data": [str(extract)]}
                            except UnicodeEncodeError:
                                try:
                                    mds[md] = {"safety": safety, "data": [extract.encode("utf-8")]}
                                except UnicodeEncodeError:
                                    try:
                                        mds[md] = {"safety": safety, "data": [extract.decode("utf-8")]}
                                    except:
                                        print
                                        print
                                        print
                                        print extract
                                        raise
        
        
        #~ html += "<br/><br/><br/><br/>"
        #~ print mds
        description = None
        image = None
        category = None
        subcategory = None
        genre = None
        episode = None
        quality = None
        title = None
        all_categories = None
        
        extracts = {}
        
        
        for md in mds:
            safety = mds[md]['safety']
            extract = ",".join(set(mds[md]['data']))

            #~ print "****", md, extract
            
            if md=="image":
                if extract != "":
                    imgs = re.findall(r'<img[^>]*\ssrc="(.*?)"', extract)
                    if imgs:
                        data[md] = imgs[0]
                        #~ html += u"<br/>\n<b>Imagen%s:</b> %s" % ("(SAFE)" if safety else "", extract)
                        image = True
            else:
                try:
                    if md == "infohash":
                        #~ print "********************"
                        extract = extract_infohash(extract)
                    if md == "category":
                        if "," in extract:
                            all_categories = extract
                            extract = extract.split(",")[0]
                        category = unicode(extract, "utf-8")
                    if md == "title":
                        title = unicode(extract, "utf-8")
                    if md == "subcategory":
                        subcategory = unicode(extract, "utf-8")
                    if md == "genre":
                        genre = unicode(extract, "utf-8")
                    if md == "description":
                        description = unicode(extract, "utf-8")
                    if md == "episode":
                        episode = unicode(extract, "utf-8")
                    if md == "quality":
                        quality = unicode(extract, "utf-8")
                    if md == "size":
                        #~ print extract
                        if not extract.isdigit():
                            extract = is_size(extract)
                        #~ if extract:
                            #~ print "size:" + extract
                    data[md] = unicode(extract, "utf-8")
                    
                    #~ html += u"<br/>\n<b>%s%s:</b> %s" % (md, u"(SAFE)" if safety else u"", unicode(extract, "utf-8"))
                except (UnicodeDecodeError, UnicodeEncodeError):
                    print "Error de codificacion (%d)" % len(extract)
                    raise 
                
                    #~ print xp
                    
         #keywords
        if title:
            rt = extract_keywords(title)
            if rt:
                data[u"keywords"] = rt
        
        if not quality and (title or "keywords" in data):
            rt = is_quality(title)
            if rt:
                data[u"quality"] = rt
            else:
                if "keywords" in data:
                    for kw in data[u"keywords"].split(","):
                        rt = is_quality(kw)
                        if rt:
                            data[u"quality"] = kw
                            break
                    
                    
        if not image and description and "src=" in description:
            imgs = re.findall(r'<img[^>]*\ssrc="(.*?)"', description)
            if len(imgs) > 0:
                #~ html += u"<br/>\n<b>Imagen(DESC):</b> <img src='%s'></img>" % (imgs[0])
                data[u"image"] = imgs[0]
                
        if not category and subcategory:
            category = get_category_from_subcategory(subcategory)
            if category:
                #~ html += u"<br/>\n<b>category(subc):</b> %s" % (category)
                data[u"category"] = category
        if not category and genre:
            category = get_category_from_genre(genre)
            if category:
                #~ html += u"<br/>\n<b>category(gen):</b> %s" % (category)
                data[u"category"] = category
                
        if not episode and title:
            rt = is_season_episode(title)
            if rt:
                #~ html += u"<br/>\n<b>episode(TIT):</b> %s" % (rt['e'])
                data[u"episode"] = rt['e']
                #~ html += u"<br/>\n<b>season(TIT):</b> %s" % (rt['s'])
                data[u"season"] = rt['s']
                if not category:
                    #~ html += u"<br/>\n<b>category(se):</b> series" 
                    data[u"category"] = "series"
        
        
        
       
            
        
        if data:
            data[u"schema"] = get_schema(data)
            
            if "category" in data:
                tags = ""
                
                if all_categories:
                    tags += ",".join([get_tags(c) for c in all_categories.split(",") if get_tags(c)])
                else:
                    tags += get_tags(data['category'])
                
                if tags:
                    if 'tags' in data:
                        data['tags'] += ",%s" % tags
                    else:
                        data['tags'] = tags
                        
        deleted = []
        for k in data:
            if isinstance(data[k], bool):
                deleted.append(k)
            else:
                data[k] = HTMLParser().unescape(data[k])
        
        for k in deleted:
            del data[k]
            
            
        return data

if __name__ == "__main__":
     me = MetaExtractor(sys.argv[1], force = True)
     print urllib.unquote(sys.argv[1]).decode("utf-8")
     
     print "links", me.get_links()
     
     
     for k,v in me.extract().items():
         print k, v
    
