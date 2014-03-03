#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii, bson, threading, sys, urllib, urllib2, gzip, StringIO
from base64 import b64decode, b32decode
import cStringIO, time
from lxml import etree
from utils import element_2_str, ugly_id
from meta import tags
import HTMLParser


class XPath(object):

    def __init__(self, url):
        self.url = url
        parser = etree.HTMLParser()
        loops = 0
        ok = False
        while not ok:
            try:
                self.tree = etree.parse(url, parser)
                ok = True
            except IOError:
                loops += 1
                if loops > 5:
                    self.tree = None
                    return None
                time.sleep(loops * 2)
                
        self.blacklist_id = ['lang', 'comment', 'browse', 'footer']
        for t in tags.values():
            self.blacklist_id.extend(t)

                
            

    def extract(self, xpath, with_images = False, extract_href = False):
        try:
            s = ""
            last = ""
            images = 0
            
            if self.tree is None:
                return ""
            
            for item in etree.XPath(xpath)(self.tree):
                
                if isinstance(item, basestring):
                    if item in last:
                        last = ""
                        continue
                else:
                    if etree.tostring(item, encoding="utf-8") in last:
                        last = ""
                        continue
                #~ if s != "":
                    #~ s += "<br/>"
                if isinstance(item, basestring):
                    s += item
                else:
                    if item.tag == "img":
                        if with_images:
                            #~ s +=  etree.tostring(item)
                            s += "<img "
                            debug = ""
                            for att, val in item.attrib.items():
                                if att == "src":
                                    if "blank." in val:
                                        continue
                                    val = val if val.startswith("http") else "http:%s "% val
                                else:
                                    if val.startswith("http") and val.endswith(".jpg"):
                                        att = "src"
                                        
                                s += ' %s="%s" ' % (att, val)
                            s += "></img>"
                        else:
                            s +=  item.attrib['src']
                    else:
                        if item.tag == "a" and extract_href:
                            s +=  item.attrib['href']
                        else:
                            #~ if "<img" in etree.tostring(item):
                                #~ continue
                            text = HTMLParser.HTMLParser().unescape(etree.tostring(item, encoding="utf-8"))
                            if with_images and "<img" in text and "data-original" in text:
                                text = text.replace("src=","src_=").replace("data-original=","src=") 
                            
                            s +=  text
                            last = text
                        
                        
            return s
        except ValueError:
            return ""
    def get_xpath(self, text_searched):

        if self.tree is None:
            #No se ha podido actualizar
            return None

        find_text = etree.XPath("//text()")
        
        
        
        #El concepto de expansivo es para buscar textos largos como la descripción y no quedarse dentro de los br o p
        self.last_expansive = False
        if len(text_searched) > 100:
            self.last_expansive = True
            
        expansive = self.last_expansive
            
        try:
            matches =  [text for text in find_text(self.tree) if text_searched in text ]
        except ValueError:
            return None
        #~ print "@@@@@@@@@@@@@@"
        #~ print text_searched, matches
        
        if not matches:
            print "No matches"
            return None
            
        match = matches[0]
        
        if len(matches) > 1:
            #~ index_matches = {}
            for m in matches:
                #si hay 2 iguales e iguales al texto buscado no es capaz de diferenciar
                #~ id_m = unicode(m,"utf-8")
                #~ if m in index_matches and m == text_searched:
                    #~ if len(text_searched) > 32:
                        #~ pass
                        #~ print "No puede discernir", len(text_searched), len(matches)
                    #~ else:
                        #~ print "No puede discernir", text_searched, matches
                    #~ return None
                #~ index_matches[m] = 1
                #Si hay varios resultados se ajusta a la que más se acerca en tamaño
                if len(match) > len(m):
                    #~ print "\t" + m
                    match = m
            
            
        xpath = None
        
        parent = match.getparent()
        
        
        xpath = "/node()" if expansive else "/text()"
        while not parent is None:
            
            path = self.tree.getpath(parent).split("/")[-1]
            
            if expansive:
                if any([path.startswith(t) and not path.startswith("bo") for t in ["br", "b", "code", "p"]]):
                    #~ print "<-"
                    parent = parent.getparent()
                    continue
                
            
            
            
            attrib = parent.attrib
            #~ print attrib
            #Si tiene clase o id puede que sea suficiente
            if "id" in attrib and not ugly_id(attrib["id"]):
                #Evita que se confunda con comentarios 
                if any([w == attrib["id"] for w in self.blacklist_id]):
                    #~ print "Ignorando %s (%s)" % (attrib['id'], xpath)
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                
                _xpath =  "//%s[@id='%s']%s"%(parent.tag, attrib["id"], xpath)
                #~ print _xpath
                find = etree.XPath(_xpath)
                #~ print find
                finded = find(self.tree)
                #Solo le deja que haya más de un match en modo expansivo y si es un id
                if (len(finded) == 1 or expansive) and match.strip() in element_2_str(finded):
                    xpath = _xpath
                    break
                else:
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
            if "class" in attrib and not ugly_id(attrib["class"]):
                
                if any([w == attrib["class"] for w in self.blacklist_id]):
                    #~ print "Ignorando %s (%s)" % (attrib['class'], xpath)
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                if len(etree.XPath("//*[@class='%s']" % attrib['class'])(self.tree)) > 1:
                    #No es una clase unica
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                
                _xpath =  "//%s[@class='%s']%s"%(parent.tag, attrib["class"], xpath)
                find = etree.XPath(_xpath)
                #~ print find
                finded = find(self.tree)
                #~ print len(finded)
                if len(finded) == 1:
                    if not match.strip() in finded:
                        _xpath =  "//%s[@class='%s']%s"%(parent.tag, attrib["class"], "/".join(xpath.split("/")[:-2]) + "/text()")
                        #~ print _xpath
                        finded = etree.XPath(_xpath)(self.tree)
                        if len(finded) == 1  and match.strip() in element_2_str(finded):
                            xpath = _xpath
                            break
                        else:
                            parent = parent.getparent()
                            continue
                    
                    xpath = _xpath
                    #~ return xpath
                    break
            
            xpath = "/%s%s"%(path, xpath)
            
            parent = parent.getparent()
            
        return xpath
        
    def get_xpath_torrents(self):
        try:
            find_text = etree.XPath("//a")
            return  { link.attrib['href']:self.short_xpath(link) for link in reversed(find_text(self.tree)) if 'href' in link.attrib and link.attrib['href'].split("?")[0].endswith(".torrent")}
        except TypeError:
            return {}
            
    def short_xpath(self, match):
        
        parent = match.getparent()
        xpath = "/%s" % self.tree.getpath(match).split("/")[-1]
        
        while not parent is None:
            #~ print parent.tag
            #~ print parent.attrib
            #~ print parent.tag
            #~ print parent.keys()
            path = self.tree.getpath(parent).split("/")[-1]
            
            
            attrib = parent.attrib
            #~ print attrib
            #Si tiene clase o id puede que sea suficiente
            if "id" in attrib and not ugly_id(attrib["id"]):
                #Evita que se confunda con comentarios 
                if any([w == attrib["id"] for w in self.blacklist_id]):
                    #~ print "Ignorando %s (%s)" % (attrib['id'], xpath)
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                
                _xpath =  "//%s[@id='%s']%s"%(parent.tag, attrib["id"], xpath)
                find = etree.XPath(_xpath)
                finded = find(self.tree)
                #Solo le deja que haya más de un match en modo expansivo y si es un id
                if len(finded) == 1:
                    return _xpath
                else:
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
            if "class" in attrib and not ugly_id(attrib["class"]):
                
                if any([w == attrib["class"] for w in self.blacklist_id]):
                    #~ print "Ignorando %s (%s)" % (attrib['class'], xpath)
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                if len(etree.XPath("//*[@class='%s']" % attrib['class'])(self.tree)) > 1:
                    #No es una clase unica
                    parent = parent.getparent()
                    xpath = "/%s%s"%(path, xpath)
                    continue
                
                _xpath =  "//%s[@class='%s']%s"%(parent.tag, attrib["class"], xpath)
                find = etree.XPath(_xpath)
                #~ print find
                finded = find(self.tree)
                #~ print len(finded)
                if len(finded) == 1:
                    return _xpath
            
            xpath = "/%s%s"%(path, xpath)
            parent = parent.getparent()
                
        #Si no ha podido recortarla devuelve la completa)
        return self.tree.getpath(match)
    
    def get_xpath_img(self, img):

        find_text = etree.XPath("//img")
        
        
        matches =  [text for text in find_text(self.tree) if "src" in text.attrib and "/".join(img.split("/")[1:]) in text.attrib['src'] ]
        #~ matches =  [text for text in find_text(self.tree)  ]
        
       
        #~ for m in matches:
            #~ print "\t" + m.attrib['src']
        
        if not matches:
            return None
            
        #~ print matches
        match = matches[0]
        
        if len(matches) > 1:
            for m in matches:
                #Si hay varios resultados se ajusta a la que más se acerca en tamaño
                if len(match) > len(m):
                    match = m
            
        #Recorta XPath
        parent = match.getparent()
        xpath = "/img"
        
        #~ print self.tree.getpath(match)
        
        while not parent is None:
            #~ print parent.tag
            #~ print parent.attrib
            #~ print parent.tag
            #~ print parent.keys()
            path = self.tree.getpath(parent).split("/")[-1]
            #~ print path
            
            attrib = parent.attrib
            #~ print attrib
            #Si tiene clase o id puede que sea suficiente
            if "id" in attrib and not ugly_id(attrib["id"]):
                _xpath =  "//%s[@id='%s']%s"%(parent.tag, attrib["id"], xpath)
                find = etree.XPath(_xpath)
                #~ print find
                finded = find(self.tree)
                if finded and finded[0].attrib['src'] in img:
                        return _xpath

            if "class" in attrib and not ugly_id(attrib["class"]):
                if len(etree.XPath("//*[@class='%s']" % attrib['class'])(self.tree)) > 1:
                    parent = parent.getparent()
                    #No es una clase unica
                    continue
                
                _xpath =  "//%s[@class='%s']%s"%(parent.tag, attrib["class"], xpath)
                #~ print _xpath
                find = etree.XPath(_xpath)
                #~ print find
                finded = find(self.tree)
                #~ print len(finded)
                if len(finded) == 1:
                    #~ print "\t" + str(finded[0])
                    #~ print finded[0].attrib
                    #~ print img
                    #~ print img in finded[0].attrib['src']
                    if finded[0].attrib['src'] in img:
                        return _xpath
            
            
            parent = parent.getparent()
                
        
        #Si no ha podido recortarla devuelve la completa
        return self.tree.getpath(match)
        
            
