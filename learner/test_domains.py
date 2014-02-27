#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, pymongo
from xpath import XPath
from utils import u, download_url
from meta import *
from meta_extractor import MetaExtractor
import sys

config = {}
execfile("learn.conf", config)
db_conn = pymongo.MongoClient(config['database_host'],config['database_port'])



_filter = {}
if len(sys.argv) > 1:
    _filter = {"_id":sys.argv[1]}

for domain in db_conn.torrents.domain.find(_filter):
    #~ print domain['_id']
    filename = "test/%s.html"%domain['_id']
    if not 'tp' in domain or not 'md' in domain:
        continue
    loop = 0
    html = u"<html><head><meta http-equiv='Content-Type' content='text/html; charset=utf-8'/></head><body><h1>%s</h1>" % u(domain['_id'])
    for url in domain['tp']:
        
        #~ if not "t7131701" in url:
            #~ continue
        time.sleep(1)
        try:
            print "\t[%d/%d]%s" % (loop, len(domain['tp']), url)
        except:
            print "\t[%d/%d]" % (loop, len(domain['tp']))
        loop += 1
        
        html += u"<h2><a href='%s' target='_blank'>%s</a></h2>"%(url, url)
        
        me = MetaExtractor(url, domain = domain, db_conn=db_conn)
        
        extracts = me.extract()
        
        #~ if extracts['category'] == "tv":
            #~ print extracts
        
        for k, v in extracts.items():
            if k == "image":
                html += u"<br/>\n<b>Imagen(DESC):</b> <img src='%s'></img>" % (v)
            else:
                html += u"<br/>\n<b>%s:</b> %s" % (k, u(v))
        continue
        
        
        xpath = XPath(url)
        if xpath is None:
            continue
        html += u"<h2><a href='%s' target='_blank'>%s</a></h2>"%(url, url)
        
        #~ print loop
        
        #~ print "*****************************************************"
        #~ print "%s [%d/%d]"%(url, loop, len(domain['tp']))
        
        #~ print "-----------------------------------------------------"
        mds = {}
        
        
        for md in domain['md']:
            #~ print "%s:"%md
            if "@url" in domain['md'][md]:
                pos = domain['md'][md].split("[")[1].split("]")[0]
                #~ print url.split("/")[int(pos)]
                html += u"<br/>\n<b>%s:</b> %s" % (u(md), u(url.split("/")[int(pos)]))
            else:
                
                extract = ""
                safety = False
                if "candidate" in domain['md'][md]:
                    extract = xpath.extract(domain['md'][md]['candidate'], True)
                    #~ if md == "image" and extract != "":
                        #~ print "IMAGE", domain['md'][md]['candidate'], extract
                    #~ print md, extract, domain['md'][md]['candidate']
                    
                    safety = True
                extracts = []
                if extract == "":
                    #Si lo ha encontrado en muchos sitios no es fiable, mejor ignorar
                    #~ print domain['md'][md]['all']
                    #~ if len(domain['md'][md]['all']) > 8:
                        #~ print "********************"
                        #~ print md
                        #~ print len(domain['md'][md]['all'])
                        #~ continue
                    
                    safety = False
                    safety_val = 0
                    #Para que un valor se considere seguro al menos un 50% de las apariciones han tenido que ser ahí
                    safety_val = sum(c for c in domain['md'][md]['all'].values() if c > 1) * 0.3
                    
                    for xp in domain['md'][md]['all']:
                        try:
                            if "comment" in xp:
                                #para que no se lie con comentarios
                                continue
                            #~ print "\t", xp
                            if "@url" in xp:
                                pos = xp.split("[")[1].split("]")[0]
                                #~ print url.split("/")[int(pos)]
                                html += u"<br/>\n<b>%s:</b> %s" % (u(md), u(url.split("/")[int(pos)]))
                                break
                            if domain['md'][md]['all'][xp] == 1:
                                #una única aparación es ignorada
                                continue
                            
                            extract = xpath.extract(xp, True)
                            #~ if md == "description":
                                #~ print
                                #~ print
                                #~ print
                                #~ print
                                #~ print md, xp, domain['md'][md]['all'][xp], len(extract)
                                #~ if len(extract)<100:
                                    #~ print extract
                            
                            
                            if extract == "":
                                continue
                            
                            #~ print "\t\t", extract
                            #~ print md, extract, xp
                            
                            extracts.append(extract)
                            #fiable si aparece mucho 
                            if domain['md'][md]['all'][xp] > safety_val:
                                safety = True
                                break
                        except (UnicodeDecodeError, TypeError):
                            continue
                
                #~ if md == "description":
                    #~ print len(extract), len(extracts), safety, md, is_valid_meta(extract, md)
                    #~ print is_script(extract)
                
                _md = {}
                
                if safety:
                    #~ print md, extracts
                    safety = is_valid_meta(extract, md)
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
                        
                        print "NOT VALID", extract if len(extract) < 100 else len(extract), md
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
        title = None
        
        
        extracts = {}
        
        
        for md in mds:
            safety = mds[md]['safety']
            extract = ",".join(set(mds[md]['data']))
            
            if md=="image":
                if extract != "":
                    html += u"<br/>\n<b>Imagen%s:</b> %s" % ("(SAFE)" if safety else "", extract)
                    image = True
            else:
                try:
                    if md == "infohash":
                        #~ print "********************"
                        extract = extract_infohash(extract)
                    if md == "category":
                        if "," in extract:
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
                    if md == "size":
                        #~ print extract
                        if not extract.isdigit():
                            extract = is_size(extract)
                        #~ if extract:
                            #~ print "size:" + extract
                    html += u"<br/>\n<b>%s%s:</b> %s" % (md,u"(SAFE)" if safety else u"", unicode(extract, "utf-8"))
                except (UnicodeDecodeError, UnicodeEncodeError):
                    html += u"<br/>\n<b>%s%s:</b> %s" % (md,"(SAFE)" if safety else "", "Error de codificacion (%d)" % len(extract))
                    raise 
                
                extracts[md] = extract
                    #~ print xp
                    
        if not image and description and "src=" in description:
            imgs = re.findall(r'<img[^>]*\ssrc="(.*?)"', description)
            if len(imgs) > 0:
                html += u"<br/>\n<b>Imagen(DESC):</b> <img src='%s'></img>" % (imgs[0])
                extracts['image'] = "<img src='%s'></img>" % (imgs[0])
                
        if not category and subcategory:
            category = get_category_from_subcategory(subcategory)
            if category:
                html += u"<br/>\n<b>category(subc):</b> %s" % (category)
                extracts["category"] = category
        if not category and genre:
            category = get_category_from_genre(genre)
            if category:
                html += u"<br/>\n<b>category(gen):</b> %s" % (category)
                extracts["category"] = category
        if not episode and title:
            rt = is_season_episode(title)
            if rt:
                html += u"<br/>\n<b>episode(TIT):</b> %s" % (rt['e'])
                html += u"<br/>\n<b>season(TIT):</b> %s" % (rt['s'])
                extracts["episode"] = rt['e']
                extracts["season"] = rt['s']
                if not category:
                    html += u"<br/>\n<b>category(se):</b> series" 
                    extracts["category"] = "series"
                
        
        for k, v in extracts.items():
            print "%s: %s" % (k,v)
        
        #~ if len(_filter)==0:
            #~ break
        #~ break
            
        #~ if loop > 10:
            #~ break
        
        #~ print "*****************************************************"
        #~ print
        #~ print
        #~ print
    html += "</body></html>"
    f = open(filename, "w")
    try:
        f.write(u(html))
    except UnicodeEncodeError:
        try:
            f.write(html.encode("utf-8"))
        except:
            try:
                f.write(html.decode("utf-8"))
            except:
                print html
                print
                print
                print
                print
                print
                raise
        
    f.close()
