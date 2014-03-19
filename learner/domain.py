#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, re, random, urllib2
import sys
from utils import download_url, save_tmp, clean_url_img, u,element_2_str, get_xpath_from_soup_object, torrent_info, is_same_torrent
from meta import *
from bs4 import BeautifulSoup
from urlparse import urlparse
import pymongo
from xpath import XPath
from PIL import Image
from datetime import datetime
from raven import Client
from urlparse import urljoin
from pprint import pprint
import operator

class Domain(object):
    def __init__(self, base_url, logger = None, db_conn = None):
        
        if logger is None:
            class l(object):
                def info(self, s):
                    try:
                        print s
                    except:
                        pass
                def error(self, s):
                    print "ERROR: %s"%s
                def warning(self, s):
                    print "WARNING: %s"%s
            self.logger=l()

        else:
            self.logger = logger
            
        if db_conn is None:
            self.db_conn = pymongo.MongoClient("localhost",27017)
        else:
            self.db_conn = db_conn
        
        
        self.base_url = base_url
        self.num_candidates = 100
        
        self.urls_torrent_page = {}
        self.urls_to_see = [self.base_url]
        self.urls_visited = []
        self.metas = {}
        self.weak_metas = ["genre", "quality", "tags", "language"]
        
        
        domain = self.db_conn.torrents.domain.find_one({"_id":self.get_id()})
        if not domain is None and 'tp' in domain:
            for url in domain["tp"]:
                self.urls_torrent_page[url] = 1
    def get_id(self):
        return urlparse( self.base_url ).netloc
        
    def save(self):
        #~ print "SAVING"
        #~ print self.get_id()
        #~ print self.metas
        self.db_conn.torrents.domain.update({"_id":self.get_id()}, {"$set":{"md":self.metas, "ls": datetime.now()}})
        if not self.metas:
            self.db_conn.torrents.domain.update({"_id":self.get_id()}, {"$inc":{"cko":1}})
    
    def learn(self):
        #Localiza paginas del sitio candidatas
        if not self.locate_torrent_pages():
            return False
        #~ #analiza sus diferencias y extrae metadatos
        if self.get_metas() is None:
            #Alguna url incorrecta. Vuelve a empezar
            self.logger.info("Alguna url ya no es valida. Relocalizando...") 
            return self.learn()
            
        self.get_image()
        self.get_category()
        self.locate_links()
        
    
        
    def add_urls(self, html):
        #Acumula todas las urls internas para seguir viendolas
        try:
            doc = BeautifulSoup(html)
        except:
            return
        
        urls = []
        for a in doc("a"):
            href = a.get("href")
            
            if href is None:continue
            href = href.split("#")[0]
            
            blacklist = ["user", "similars", "recent", "request", "community", "comments", "javascript", "login", "details", "blog"]
            
            if (href.startswith(self.base_url) or not href.startswith("http")) and not any(w in href for w in blacklist):
                urls.append(href)
        
        if urls:
            random.shuffle(urls)
            self.urls_to_see.extend(urls)
                
    def is_torrent_page(self, html, url):
        
        if html is None or len(html) == 0:
            try:
                self.logger.info("%s: html vacio!" % url)
                print "%s: html vacio!" % url
            except:
                pass
            return False
        
        blacklist_url = ["search", "category", "browse"]
        if any([w in url for w in blacklist_url]):
            self.logger.info("blacklist en la url")
            print "blacklist en la url"
            return False 
                
        #Es una página de torrrent si solo tiene enlaces a un unico torrent(enlaces al .torrent o al magnet)
        try:
            doc = BeautifulSoup(html)
        except:
            self.logger.info("No procesado por beautifulsoup")
            print("No procesado por beautifulsoup")
            return False
        
        torrents = []
        magnets = []
        
        for a in doc("a"):
            href = a.get("href")
            
            if not href is None and href.startswith("magnet:"):
                magnets.append(href)
            if not href is None and href.endswith(".torrent"):
                torrents.append(href)
        
        #Busca algún infohash 
        visible_text = " ".join(s for s in doc.stripped_strings if not is_script(s)).lower()
        
        rt = re.findall("(\W)([0-9a-fA-F]{40})(\W)",visible_text)
        
        #Alguna palabra de esta lista debe estar
        whitelist = ["size", "hash","seed", "leech", "seeders", "leechers"]
        if not any([w in visible_text for w in whitelist]):
            self.logger.warning("No tiene whitelist en el contenido")
            return False
        
        #Alguna palabra de esta lista no debe estar
        blacklist = ["torrent found", "torrents found"]
        if any([w in visible_text for w in blacklist]):
            self.logger.warning("tiene blacklist en el contenido")
            return False
        
        
        ihs = [ih[1] for ih in rt]
        
        #~ if len(ihs) == 1:
            #~ for s in doc.stripped_strings:
                #~ print s
            #~ exit()
        
        return  (len(magnets) == 1 or len(torrents) == 1) or len(set(ihs)) == 1
        #~ return  len(set(ihs)) == 1
        
    def all_torrent_pages_ok(self):
        
        if len(self.urls_torrent_page) < self.num_candidates:
            return False
        
        #~ return True
        matrix = []
        bads = []
        #quitando el final(comunmente el nombre, deben aparecer el mismo número de "_" y "-"
        matrix_ =[]
        matrix_g =[]
        
        name_in_last = -2 if self.urls_torrent_page.keys()[0].endswith("/") else -1
        
        for url in self.urls_torrent_page:
            matrix.append(url.split("/"))
            matrix_.append("/".join(url.split("/")[:name_in_last]).split("_"))
            matrix_g.append("/".join(url.split("/")[:name_in_last]).split("-"))
            
        
        #Se queda solo con los tamaños más comunes
        sizes = {}
        sizes_ = {}
        sizes_g = {}
        
        
        for m in matrix:
            sz = str(len(m))
            if not sz in sizes:
                sizes[sz] = 0
            sizes[sz] += 1
        for m in matrix_:
            sz = str(len(m))
            if not sz in sizes_:
                sizes_[sz] = 0
            sizes_[sz] += 1
        for m in matrix_g:
            sz = str(len(m))
            if not sz in sizes_g:
                sizes_g[sz] = 0
            sizes_g[sz] += 1
        

        max_count = 0
        for c, count in sizes.items():
            if count > max_count:
                size = int(c)
                max_count = count
        
        max_count = 0
        for c, count in sizes_.items():
            if count > max_count:
                size_ = int(c)
                max_count = count
        
        max_count = 0
        for c, count in sizes_g.items():
            if count > max_count:
                size_g = int(c)
                max_count = count
        
        
        #descarta urls por no ser homogeneas
        for url in self.urls_torrent_page:
            if len("/".join(url.split("/")[:name_in_last]).split("_")) != size_:
                self.logger.info( "bad size_:%s [%d] %s"%(size_, len("/".join(url.split("/")[:name_in_last]).split("_")), url))
                bads.append(url)
            if len("/".join(url.split("/")[:name_in_last]).split("-")) != size_g:
                self.logger.info( "bad size-g:%s [%d] %s"%(size_g, len("/".join(url.split("/")[:name_in_last]).split("-")), url))
                bads.append(url)
            if len(url.split("/")) != size:
                self.logger.info( "bad size:%s [%d] %s"%(size, len(url.split("/")), url))
                bads.append(url)
            if len(re.findall(r"[\w']+", url.replace(self.base_url,"").replace("_"," "))) < 5:
                self.logger.info("bad pocas palabras:%s"%(url))
                bads.append(url)
                
        
        starts = {}
        
        
        #Si todas son malas no se va a poder hacer nada mejor, se deja pasar
        if len(bads) < self.num_candidates:
            if bads:
                for b in bads:
                    if b in self.urls_torrent_page:
                        del self.urls_torrent_page[b]
                return False
        
        return True
        
        
    def locate_torrent_pages(self):
        #Recorre el sitio en busca de las páginas de fichero
        
        while len(self.urls_torrent_page) < self.num_candidates and self.urls_to_see:
            
            url = self.urls_to_see.pop()
            
            if not url.startswith("http"):
                url = "%s%s%s"%(self.base_url, "" if url.startswith("/") else "/", url)
            
            if url in self.urls_visited:
                continue
            
            
            self.logger.info(u"Recorriendo %s"%url)
            html = download_url(url)
            
            
            self.add_urls(html)
            if self.is_torrent_page(html, url):
                
                self.urls_torrent_page[url] = 1
                self.logger.info("torrent page localizada (%d)"%len(self.urls_torrent_page))
                
            self.urls_visited.append(url)
            if len(self.urls_visited) % 100 == 0:
                self.logger.info("%d pages visited, %d torrents page located" % (len(self.urls_visited), len(self.urls_torrent_page)))
                
            if len(self.urls_visited) > 20000 or (len(self.urls_visited) > 1000 and len(self.urls_torrent_page) < (len(self.urls_visited)/1000)): 
                return False
            #Con calma para evitar baneos
            time.sleep(3)
            
            
        if len(self.urls_torrent_page) < self.num_candidates:
            self.logger.info(len(self.urls_torrent_page))
            self.logger.info(self.urls_to_see)
            return False
            #~ raise Exception("no se han encontrado candidatas")
            
        if not self.all_torrent_pages_ok():
            self.locate_torrent_pages()
        
        
        #Guarda en db
        self.db_conn.torrents.domain.update({"_id":self.get_id()},{"$set":{"tp":self.urls_torrent_page.keys()}},upsert=True)
        
        return self.urls_torrent_page
        
        
    def get_metas(self):
        self.logger.info("Busqueda de metadatos")
        
        data = {}
        l = 0
        
        if not self.urls_torrent_page:
            return False
        
        for url in self.urls_torrent_page:
            
            self.logger.info("[%d/%d]Extrayendo cadenas de %s"%( l, len(self.urls_torrent_page), url))
            l += 1
            try:
                html = download_url(url)
                doc = BeautifulSoup(html)
            except TypeError:
                self.logger.warning("No se ha podido cargar %s"%url)
                continue
                
            if not self.is_torrent_page(html, url):
                del self.urls_torrent_page[url]
                self.logger.warning("No es torrent page")
                return None

            if doc is None or doc.body is None:
                del self.urls_torrent_page[url]
                self.logger.warning("El doc obtenido no es valido")
                return None
                    
            strings = []
            for string in doc.body.stripped_strings:
                strings.append(string)
            data[url] = strings
            
        map_str = []
        for pos in xrange(0, len(strings)): 
            self.logger.info("Analizando [%d/%d]"%(pos, len(strings)))
            equal = True
            first = True
            previous = None
            for url in data:
                if previous is None:
                    previous = url
                else:
                    try:
                        if len(data[url]) < pos or len(data[previous]) < pos  or (data[url][pos] != data[previous][pos]):
                            if first:
                                first = False
                                continue
                            equal = False
                            break;
                    except IndexError:
                        if first:
                            first = False
                            continue
                        equal = False
                        break; 
            
                
            map_str.append(equal)
        
        
        #~ data = {}
        rt = {}
        duplicated = []
        
        self.logger.info("Busqueda atributos")
        
        #busqueda de atributos para cada url
        for url in self.urls_torrent_page:
            
            #~ self.logger.info("Procesando cadenas %s"%url)
            
            pos = 0
            last_equal = True
            
            metadata = {"infohash" : None,
                        "size" : None,
                        "description" : None,
                        "title" : None, 
                        "category" : None,
                        "tags" : None,
                        "quality" : None,
            
                        "genre" : None,

                        #~ "series" : None,
                        "season" : None,
                        "episode" : None,
            
                        "language" : None}
            
            
            pos = 0
            last_equal = True
            
            next_is_description = False
            
            doc = BeautifulSoup(download_url(url))
            try:
                h1 = doc("h1")[0].stripped_strings.next()
                if len(doc("h1"))> 0 and is_title(h1, url, full = True):
                    metadata['title'] = h1
                else:
                    h2 = doc("h2")[0].stripped_strings.next()
                    if doc("h2") and is_title(h2, url, full  = True):
                        metadata['title'] = h2
            except IndexError:
                pass
                    
            
            #~ print metadata['title']
            
            meta = {}
            for equal in map_str:
                #Lo que parecen metadatos que ofrece la página
                if not equal and last_equal:
                    #~ print pos, "[%s]"%data[self.urls_torrent_page[0]][pos-1], data[self.urls_torrent_page[0]][pos]
                    if pos>0:
                        prev = data[url][pos-1]
                        token = data[url][pos]
                        if len(prev) < 20 and not prev.isnumeric() and len(prev) > 2 and prev.count(" ")<2:
                            meta[prev.replace(":","").lower()]  = token
                pos += 1
                last_equal = equal
                
            for m in metadata:
                if m in meta:
                    metadata[m] = meta[m]
            
            pos = 0
            last_equal = True
            
            
            #~ print url
            #~ print metadata
            #~ print "*"*44
            #Busqueda en bruto
            already_title = False
            
            search_tags = True
            
            for equal in map_str:
                
                if pos < len(data[url]):
                    token = data[url][pos]
                
                #~ print token
                #Hasta que no aparece el titulo no empieza a buscar
                if not already_title:
                    if not(len(token) > 5 and is_title(token.lower(), url, full = True)):
                        pos += 1
                        continue
                    already_title = True
                
                #~ print "*****************"
                
                #La descripción suele estar al final
                ending = ["comment", "related", "similar"]

                if any([w in token.lower() for w in ending]) and pos > len(data[url]) * 0.4:
                    #~ print pos, len(data[url])
                    #nada interesante despues de los comentarios o archivos relacionados
                    #~ exit()
                    #~ print token
                    #~ print "*********ENDING***************"
                    break
                    
                if search_tags:
                    if any([w in token.lower() for w in ending]):
                        search_tags = False
                
                if pos > (len(map_str) * 0.75):
                    
                    #Esto es el final de la página y ya no hay nada interesante
                    #~ print token
                    #~ print "*********ENDING LARGE***************"
                    break
                
                if is_script(token):
                    pos += 1
                    last_equal = equal
                    continue
                if not equal:
                    if search_tags:
                        tag = is_tag(token)
                        if tag:
                            if metadata['tags'] is None or isinstance(metadata['tags'], basestring):
                                metadata['tags'] = {}
                            
                            if not tag in metadata['tags']:
                                try:
                                    metadata['tags'][tag] = []
                                except:
                                    print metadata
                                    print metadata['tags']
                                    print tag
                                    raise
                            if not token in metadata['tags'][tag]:
                                metadata['tags'][tag].append(token)
                    
                    
                    if metadata['description'] is None or len(metadata['description']) < len(token):
                        #busca descripciones cortas salvo que las haya más largas
                        if len(token) > 100 or next_is_description:
                            #~ print url
                            #~ print "--------------"
                            #~ print "token",token
                            #~ print "desc_candidate", desc_candidate
                            if not equal and not is_script(token) and is_description(token):
                                metadata['description'] = token
                                #~ print url
                                #~ print "metadata['description']", metadata['description']
                            next_is_description = False
                            
                    
                    if "description" in token:
                        next_is_description = True
                        
                    if metadata['title'] is None:
                        if len(token) > 5 and is_title(token, url):
                            metadata['title'] = token

                    if metadata['season'] is None or metadata['episode'] is None:
                        if len(token) > 3:
                            se = is_season_episode(token)
                            if se:
                                metadata['season'] = {"token" : token, "value" : se['s']}
                                metadata['episode'] = {"token" : token, "value" : se['e']}
                                

                    if metadata['size'] is None:
                        if len(token) > 2:
                            z = is_size(token)
                            if z:
                                metadata['size'] = {"token" : token, "value":z}

                    if metadata['infohash'] is None:
                        if "hash" in token or len(token) == 40:
                            metadata['infohash'] = extract_infohash(token)
                    
                    
                    #~ if metadata['category'] is None:
                        #~ if is_category(token):
                            #~ print url
                            #~ print "\t\t\t", token
                            #~ print get_xpath_from_soup_object(token)
                            #~ metadata['category'] = token
                    
                    if metadata['language'] is None:
                        if is_language(token):
                            metadata['language'] = token
                
                pos += 1
                last_equal = equal
            
            #~ print
            #~ print
            #~ print url
            #~ print metadata
            #~ print "--"
            #~ print url
            #~ if not metadata['tags'] is None:
                #~ print metadata['tags']
            
            #~ self.logger.info("Extrayendo xpaths %s"%url)
            xpath = XPath(url)
            if not xpath:
                del self.urls_torrent_page[xpath]
                self.logger.warning("No se pueden xpathear %s" % url)
                return None
            
            
            _metadata = {}
            
            
            for m, v in metadata.items():
                
                #~ print m, v
                
                if v:
                        
                    if type(v) == type({}):
                        if not "value" in v:
                            value = ",".join([",".join(keywords) for keywords in v.values()])
                            token = value.split(",")
                            #~ print token
                        else:
                            value = v['value']
                            token = v['token']
                    else:
                        value = v
                        token = v
                    
                    def extract_token(token, value):
                        #~ print "extrayendo %s - %s"%(token, value)
                        try:
                            xp = xpath.get_xpath(u(token)) 
                        except UnicodeDecodeError:
                            #~ print "unicode"
                            return False
                        
                        if xp is None:
                            #~ print "xp"
                            return False
                        #~ print "XP", xp
                        extract = xpath.extract(xp)
                        if not extract:
                            #~ print "no extract", token, xp 
                            return False
                        
                        #~ print ".."
                        #~ print element_2_str(extract)
                        
                        #~ print "TOKEN-EXTRACT", token.strip(), extract.strip()
                        
                        if len(extract) > 0 or not token.strip() == extract.strip():
                            #~ print "[%s][%s]"%(token, extract[0])
                            ok = True
                            if xpath.last_expansive:
                                ok = False
                                try:
                                    if token.strip() in element_2_str(extract):
                                        #es correcto, probablemente la descripcion
                                        ok = True
                                    else:
                                        #~ self.logger.warning("else last_expansive")
                                        return False
                                except:
                                    return False

                            if not ok:
                                if not token.strip() in extract.strip():
                                    print("No coincide %s[%s] para %s"%(xp, extract, token))
                                    self.logger.error("No coincide %s[%s] para %s"%(xp, extract, token))
                                    raise Exception("Incoherencia xpath")
                                
                                #~ self.logger.warning("No se puede extrar el xpath de %s en %s"%(token, url)    )
                                return False
                            
                        
                        
                        id_m = m
                        if m == "tags":
                            tg = is_tag(value)
                            if tg:
                                id_m = tg.split("_")[0]
                        
                        if id_m in _metadata and _metadata[id_m]['xpath'] != xp and id_m != "category":
                            duplicated.append(id_m)
                        
                        #Para el metadata "language" no se guardan xpath de enlaces ya que suelen ser selectores de idioma de la página
                        if id_m == "language" and "/a/" in xp:
                            return False 
                        
                        #h1 solo para el title
                        if id_m != "title" and "/h1" in xp:
                            return False
                        
                        if "'tab-main'" in xp and id_m == "subcategory":
                            print
                            print
                            print
                            print url
                            print id_m
                            print _metadata
                            print value
                            print xp
                            exit()
                            
                        
                        #No se guarda nada que cuelgue de comentarios, script o style
                        invalid = ["comment", "script", "style", "select"]
                        
                        
                        if not any([w in xp for w in invalid]) :
                            _metadata[id_m] = {"value" : value, "xpath" : xp}
                            
                        #~ print "_METADATA", _metadata
                    
                    if type(token) == type([]):
                        for t in token:
                            #~ print "\t"+t
                            extract_token(t, t)
                    else:
                        extract_token(token, value)
                
            #evita confundir description con title
            if "description" in _metadata and "title" in _metadata:
                if _metadata['title']['xpath'] == _metadata['description']['xpath']:
                    del _metadata['description']
            
            #~ print "%s:%s"%(url, _metadata)
            #~ exit()
            for d in duplicated:
                if d in _metadata:
                    del _metadata[d]
            
            duplicated = []
            
            rt[url] = { "metadata" : _metadata, "meta" : meta}
            
            #~ pprint(rt[url]['metadata'])
            #~ exit()
            #~ print
            #~ print
            #~ print
            #~ print "**********************"
            #~ print url
            #~ print rt[url]['metadata']
            #~ print "**********************"
            

        #~ for url, d in rt.items():
            #~ print url
            #~ for k,v in d['metadata'].items():
                #~ if not v is None:
                    #~ print "%s: {%s:%s}"%(k, v['value'], v['xpath'])
                #~ 
            #~ print
            #~ for k, v in d['meta'].items():
                #~ if not v is None:
                    #~ print "[%s] "%(k)
            #~ print 
            #~ print "**************"
        
        
        metas_ocurrences = {}
        for url, d in rt.items():
            #~ print url
            for k,v in d['metadata'].items():
                #~ print k, v
                if not k in metas_ocurrences:
                    metas_ocurrences[k] = {}
                value = v['xpath']
                if not value in metas_ocurrences[k]:
                    metas_ocurrences[k][value] = 0
                metas_ocurrences[k][value] += 1
        
        #~ print "ocurrences"
        #~ print metas_ocurrences
        
        
        metas = {}
        for m, xpaths in metas_ocurrences.items():
            for xpath, count in xpaths.items():
                #~ if count > (self.num_candidates / 10):
                #~ if count > 0:
                if not m in metas:
                    metas[m] = {}
                    metas[m]["all"] = {}
                if not xpath in metas[m]["all"]:
                    metas[m]["all"][xpath] = count
                #~ metas[m]["all"] = "(%d)%s"%(count, xpath)
                
        #busca en los metas "debiles" y los elimina si no está seguro de que son correctos
        
        for weak in self.weak_metas:
            if weak in metas:
                ok = False
                sum_counts = sum(metas[weak]['all'].values())
                for count in metas[weak]['all'].values():
                    if count > (sum_counts / 3):
                        ok = True
                if not ok:
                    del metas[weak]
        
                    
        
        
        self.metas = metas
                  
        return True
        

    def get_image(self, mode = 0):
        #-----------------
        #Valores para mode. Se utiliza para considerar una imagen o no candidata por el tamaño
        #0 -> width y height > 100
        #1 -> width y height >= 100
        #2 -> width o height > 100
        #3 -> width o height >= 100
        
        if mode>3: return None
        
        
        data = {}
        loop = 0
        
        blacklist = ["avatar", "promo", "category", "categories", "user", "ads"]
        
        domain = self.get_id()
        images = {}
        
        for url in self.urls_torrent_page:
            self.logger.info("Analizando %s en busca de imagenes [%d/%d]"%(url, loop, len(self.urls_torrent_page)))
            loop += 1
            #~ if loop > 10:
                #~ break
            doc = BeautifulSoup(download_url(url))
            imgs = []

            xpath = XPath(url)
            title = None
            
            if xpath:
            
                if 'title' in self.metas:
                    for xp in self.metas['title']['all']:
                        extract = xpath.extract(xp)
                        rt = is_title(extract, url)
                        if rt:
                            title = extract
                                
                                
            for img in doc("img"):
                
                #~ print title, img.get("alt"), img.get("src")
                
                
                if img.get("alt") and title and title in img.get("alt"):
                    images[url] = {"img":img.get("src"), "xpath":xpath.get_xpath_img(clean_url_img(img.get("src"), domain))}
                    self.logger.info("Imagen localizada %s"%images[url])
                    #the one
                    imgs = [img.get("src")]
                    break
                
                imgs.append(img.get("src"))
                
            
            
            data[url] = imgs
                
        
        commons = []
        
        
        
        
        for url in data:
            for img in data[url]:
                if not img in images:
                    images[img] = 0
                images[img] += 1
        
        for img, count in images.items():
            if count>(len(self.urls_torrent_page) / 2):
                commons.append(img)
        
        
        images = {}
        loop = 0
        for url in self.urls_torrent_page:
            self.logger.info("Buscando la principal en %s [%d/%d]"%(url, loop, len(self.urls_torrent_page)))
            loop += 1
            
            #~ if loop > 10:
                #~ break
            img_candidates = {}
            
            for img in data[url]:
                if not img in commons and not ".." in img and not any([w in img.lower() for w in blacklist]):
                    try:
                        self.logger.info("Salvando temporal de %s" % img)
                        im = Image.open(save_tmp(img, domain))
                        width, height = im.size
                        print img, width, height
                        #ver comentario al principio de la funcion para el comportamiento de mode
                        if mode == 0:
                            if width > 100 and height > 100:
                                img_candidates[img] = "%sx%s"%(width, height)
                        if mode == 1:
                            if width >= 100 and height >= 100:
                                img_candidates[img] = "%sx%s"%(width, height)    
                        if mode == 2:
                            if width > 100 or height > 100:
                                img_candidates[img] = "%sx%s"%(width, height)
                        if mode == 3:
                            if width >= 100 or height >= 100:
                                img_candidates[img] = "%sx%s"%(width, height)    
                        
                       

                    except IOError as e:
                        self.logger.error("IOError %s: %s "%(e, img))
                        pass
                        
            
            #~ print img_candidates
             #Si se repiten tamaño se anulan
            def size_equal(s1, s2):
                ss1 = s1.split("x")
                ss2 = s2.split("x")
                return abs(int(ss1[0])-int(ss2[0])) < 3 and abs(int(ss1[1])-int(ss2[1])) < 3
            
            
            #~ print "*********"
            #~ pprint(img_candidates)
            
            no_candidates = []
            for img in img_candidates:
                size = img_candidates[img]
                for img2 in img_candidates:
                    if img != img2 and size_equal( size, img_candidates[img2]):
                        no_candidates.append(img)
            
            
            for no in no_candidates:
                if no in img_candidates:
                    del img_candidates[no]
            
            
            #~ pprint.pprint(img_candidates)
            no_candidates = []
            if len(img_candidates) > 1:
                #Intenta excluir las que no son del propio dominio
                for img in img_candidates:
                    if not domain in img:
                        no_candidates.append(img)
            
            
            
            for no in no_candidates:
                if no in img_candidates:
                    del img_candidates[no]

            
    
            images[url] = None    
            if len(img_candidates) == 1:
                #Imagen obtenida
                xpath = XPath(url)
                img = clean_url_img(img_candidates.keys()[0], domain)
                
                
                images[url] = {"img":img, "xpath":xpath.get_xpath_img(img)}
                
                self.logger.info("Imagen localizada %s"%images[url])
                
            
        xpaths = {}
        #Recuenta todos los xpaths que han aparecido
        for img, v in images.items():
            if v is None:
                continue
            xpath = v['xpath']
            
            if xpath:
                #ignora logos
                if 'logo' in xpath:
                    continue
                
                if not xpath in xpaths:
                    xpaths[xpath] = 0
                xpaths[xpath] += 1
        
        
        #Se queda con el más comun si aparece un minimo de veces
        current_xpath = None
        max_count = 0
        for xpath, count in xpaths.items():
            #~ print count, max_count
            if count > max_count:
                max_count = count
                current_xpath = xpath
        
        
        #~ print current_xpath
        #~ print max_count , (len(self.urls_torrent_page) / 10)
        #~ if max_count >= (len(self.urls_torrent_page) / 10):
        if max_count >= 1:
            #Un xpath suficiente
            self.metas['image'] = {"candidate":current_xpath, "all":xpaths}
            return current_xpath
        
        
        return self.get_image(mode + 1)
    
    
    def get_category(self):
        
        
        
        if 'category' in self.metas and 'all' in self.metas['category'] and sum(v for v in self.metas['category']['all'].values()) > (len(self.urls_torrent_page)/3):
            print "****************************"
            print self.metas['category']
            print "ya tiene"
            return True
            
        if not self.urls_torrent_page:
            return False
        
        blacklist = ["user", "download", ".torrent","magnet", "api", "about", "privacy", "register", "contact", "recover"
                , "latest", "popular", "request", "rss", "faq"]
        
        data = {}
        for url in self.urls_torrent_page:
            #~ print url
            doc = BeautifulSoup(download_url(url))
            #~ print "cargando doc", url
            links = []
            
            #Busca enlaces que parezcan categorias
            for link in doc("a"):
                href = link.get("href")
                if href is None:
                    continue
                if href.startswith("/") and not href.startswith("//"): href = "/".join(url.split("/")[:3]) + href
                if not any([w in href.lower() for w in blacklist]) and href.startswith("/".join(url.split("/")[:3])) and href != "/".join(url.split("/")[:3]):
                    if not href in links and is_category(link.string):
                        xp = get_xpath_from_soup_object(link)
                        links.append((href, link.string, xp))
                        
            data[url] = links   

        map_links = {}
        for url, links in data.items():
            
            #~ print url, links
            pos = 0
             
            for link in links:
                #~ print link
                if not pos in map_links.keys():
                    map_links[pos] = []
                    
                _id = "%s|||%s|||%s" % link 
                if not _id in map_links[pos]:
                    map_links[pos].append(_id)
                    
                
                pos += 1
                #~ print "\t:%s, %s, %s"%link
        
        
        
        xp = None
        for pos, xpaths in map_links.items():
             #~ print pos, len(xpaths), xpaths
             if len(xpaths) > 1:
                xp = xpaths[0].split("|||")[-1]
                break
        
        
        if not xp:
            xpath_cat = {}
            
            for url in self.urls_torrent_page:
                #Lo intenta buscando breadcrumb
                _doc = BeautifulSoup(download_url(url))
                _xpath = XPath(url)
                
                next_cat = False
                for string in _doc.stripped_strings:
                    
                    if next_cat:
                        if is_category(string):
                            _xp =  _xpath.get_xpath(string)
                            if not _xp in xpath_cat:
                                 xpath_cat[_xp] = 0
                            xpath_cat[_xp] += 1
                            
                            
                        next_cat = False
                    if u">" in string and not u"<" in string or u"»" in string and not is_script(string):
                        next_cat = True
                
            for _xp in xpath_cat:
                if xpath_cat[_xp] > (len(self.urls_torrent_page) * 0.75):
                    xp = _xp
            
            
            
        if not xp:
            #Lo intenta en la url
            pos = 0
            for url_part in url.split("/"):
                if (is_category(url_part)):
                    #~ print "category(url)", url_part
                    xp = "@url[%d]"%pos
                pos += 1
        
        
        
        #~ print "..........."
        #~ print url.split("/")[2]
        #~ print url
        
        
        #~ print xp
        _all = {} 
        for url in self.urls_torrent_page:
            if not xp is None:
                if "@url" in xp:
                    if not xp in _all:
                        _all[xp] = 0
                    _all[xp] += 1
                    #~ self.metas['category'] = xp
                    #~ return xp
                try:
                    extract = XPath(url).extract(xp)
                except:
                    continue
                
                
                if len(extract) > 0 and is_category(extract):
                    if not xp in _all:
                        _all[xp] = 0
                    _all[xp] += 1
                    #~ self.metas['category'] = xp
                    #~ print self.metas['category']
                    #~ exit()
                    #~ return xp
        if not "category" in self.metas:
            self.metas['category'] = {}
        if not "all" in self.metas['category']:
            self.metas['category']['all'] = {}
            
        for a in _all:
            #Si ha encontrado la mayoria con este metodo elimina el resto
            if _all[a] > (len(self.urls_torrent_page) * 0.6):
                self.metas['category']['all'] = {}
                
            if not a in self.metas['category']['all']:
                self.metas['category']['all'][a] = 0
            
            self.metas['category']['all'][a] += _all[a]
        #Si hay alguno con @url solo vale ese
        for a in self.metas['category']['all']:
            if "@url" in a:
                count = self.metas['category']['all'][a]
                self.metas['category']['all'] = {}
                self.metas['category']['all'][a] = count
        
        return _all
                
    
    def locate_links(self):
        """download all torrents and get xpaths of torrent links"""
        links = {}
        loop = 0
        for url in self.urls_torrent_page:
            
            self.logger.info("Buscando enlaces en %s [%d/%d]"%(url, loop, len(self.urls_torrent_page)))
            loop += 1
            
            xpath = XPath(url)
            
            if xpath:
            
                data = {}
                #extrae datos que tambien vienen dentro del torrent
                for k in ['size', 'infohash', 'title']:
                    if k in self.metas:
                        xps = self.metas[k]['all']
                        sorted_xp = reversed(sorted(xps.iteritems(), key=operator.itemgetter(1)))
                        for xp_tuple in sorted_xp:
                            xp = xp_tuple[0]
                            extract = xpath.extract(xp)
                            #~ print "\t", k, extract, xp
                            rt = is_valid_meta(extract, k)
                            if rt:
                                data[k] = extract if k == "title" else rt
                                break
                
                
                
                
                #y se asegura de que coincidan
                for url_torrent, xp in xpath.get_xpath_torrents().items():
                    
                    
                    url_torrent = urljoin(self.base_url, url_torrent)
                    
                    #~ print "@"*22
                    #~ print url_torrent, xp
                    
                    tr = download_url(url_torrent, force = True) #, verbose = True)
                    if tr:
                        #~ print tr
                        #~ print len(tr)
                        try:
                            info = torrent_info(tr)
                        except:
                            continue
                        if is_same_torrent(data, info): #, verbose = True):
                            if not xp in links:
                                links[xp] = 0
                            #~ print xp
                            links[xp] += 1
                
                data[url] = links
            self.metas['links'] = {}        
            self.metas['links']['all'] = {k:v for k,v in links.items() if v>(max(v for v in links.values()) / 3)}
                
        
if __name__ == "__main__":
    
    d = None
    config = {}
    execfile("learn.conf", config)
    sentry_client = Client(config['sentry_client'])
    db_conn = pymongo.MongoClient(config['database_host'],config['database_port'])
    if len(sys.argv) > 1 and not "no-crawler" in sys.argv:
        for do in sys.argv[1:]:
            d = Domain(do, db_conn = db_conn)
            #~ url = "http://extratorrent.cc/torrent/2738131/Babes+-+Shazia+Sahari++%2A%2A++NEW++9+july+2012++%2A%2A++HD+720.html"
            #~ print len(download_url(url, verbose= True))
            #~ print d.is_torrent_page(download_url(url), url)
            d.learn()
            d.save()
    else:
        try:
            for domain in db_conn.torrents.domain.find():                
                if 'ls' in domain and (datetime.now() - domain['ls']).days < 2:
                    print "Ignorando %s por haberlo intentado hace poco" % domain['_id']
                    continue
                if 'cko' in domain and domain['cko'] > 5:
                    print "Ignorando %s Por haberlo intentado muchas veces sin exito " % domain['_id']
                    continue
                if "ok" in domain and domain['ok']:
                    print "Ignorando %s por estar validado" % domain['_id']
                    continue
                if "no-crawler" in sys.argv and not "tp" in domain:
                    print u"Ignorando %s por no estar crawleado y haberlo indicado así" % domain['_id']
                    continue
                #comprueba que el dominio no redirecciona
                if domain["_id"].endswith("/") and db_conn.torrents.domain.find_one({"_id":domain["_id"][:-1]}):
                    print u"Ignorando %s por duplicado con /" % domain['_id']
                    continue
                
                try:
                    url = urllib2.urlopen("http://%s" % domain['_id']).geturl()
                    print url
                    if not domain['_id'] in url:
                        print u"Ignorando %s por redireccionar a otro dominio " % domain['_id']
                        domain_id = url.split("://")[1].replace("www.","")
                        if not db_conn.torrents.domain.find_one({"_id":domain_id}):
                            db_conn.torrents.domain.save({"_id":domain_id})
                        continue
                except:
                    pass
                #~ print domain['tp']
                print "*******************************************"
                print "Examinando", domain['_id']
                d = Domain("http://%s" % domain['_id'], db_conn = db_conn)
                d.learn()
                d.save()
                print "Completado", domain['_id']
            #~ d = Domain("http://kickass.to")
            #~ d = Domain("http://fenopy.se")
            #~ d = Domain("http://torrents.net")
            #~ d = Domain("http://www.sumotorrent.sx")
            #~ pass
        except:
            sentry_client.captureException()
    
    if not d is None:
        for k, v in d.metas.items():
            print k,v
        
    else:
        print "Nothing to do"
    #~ urls = d.locate_torrent_pages()
    #~ for u in urls:
        #~ print u
    #~ print d.is_torrent_page(download_url("http://fenopy.se/torrent/man+of+steel+2013+1080p+brrip+x264+yify/MTA4ODU4NTA"))
    
