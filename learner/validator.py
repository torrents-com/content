#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, pymongo
from xpath import XPath
from utils import u, download_url, strip_tags
from meta import *
from meta_extractor import MetaExtractor
import sys, os, urllib2
from datetime import datetime
from raven import Client
from datetime import datetime

config = {}
execfile(os.path.join(os.path.dirname(__file__),"learn.conf"), config)
db_conn = pymongo.MongoClient(config['database_host'],config['database_port'])


#activated if there are many changes in meta.py definitions
changes_allowed = False

_filter = {}
if len(sys.argv) > 1:
    _filter = {"_id":sys.argv[1]}

rs = {}

sentry_client = Client(config['sentry_client'])
try:
    
    #para evitar cierres del cursor
    domains = [d for d in db_conn.torrents.domain.find(_filter)]
    
    #~ for domain in db_conn.torrents.domain.find(_filter).batch_size(16):
    for domain in domains:
        data = {}
        #~ print domain['_id']
        filename = "test/%s.html"%domain['_id']
        if not 'md' in domain:
            #pendiente de ser visto por el analizador de sitios
            continue
        if not 'tp' in domain:
            #No es valido ni lo va a ser pero se ha intentado
            print domain['_id'], " no es valido"
            rs[domain['_id']] = False
            continue
        
        try:
            url_redir = urllib2.urlopen("http://%s" % domain['_id']).geturl()
            if domain['_id'][-1] == "/" and url_redir[-1] != "/":
                url_redir = "%s/" % url_redir
            if domain['_id'][-1] != "/" and url_redir[-1] == "/":
                url_redir = url_redir[:-1]


            print domain['_id']
            print url_redir
            if url_redir != "http://%s" % domain['_id']:
                print "buscando duplicado... "
                #Si a la que redirecciona la tenemos este no es valido
                if db_conn.torrents.domain.find_one({"_id":url_redir.split("//")[1]}):
                    print domain['_id'], " no es valido por duplicado"
                    rs[domain['_id']] = False
                    continue
        except:
            pass
        
            
        loop = 0
        for url in domain['tp']:
            try:
                print "\t[%d/%d]%s" % (loop, len(domain['tp']), url)
            except:
                print "\t[%d/%d]" % (loop, len(domain['tp']))
            loop += 1
            me = MetaExtractor(url, domain = domain)
            
            data[url] = me.extract()
            if not data[url]:
                print "no se ha podido extraer de %s"%url
                del data[url]
                continue
                
        if not data:
            #No es valido ni lo va a ser pero se ha intentado
            print domain['_id'], " no es valido"
            rs[domain['_id']] = False
            continue
        
        if not url in data or not data[url]:
            print "no se ha podido extraer de %s"%url
            continue
            
            #~ print 
            #~ print
            #~ print "*"*32
            #~ print url
            #~ print "-"*32
            #~ for k, v in data[url].items():
                #~ if k!="description":
                    #~ print k, v
            #~ print "*"*32
            #~ print  
            


        #No valida si todos tienen un atributo siempre igual, excluida la categoria 
        keys = {}
        
        for key in data[data.keys()[0]].keys():
            keys[key] = False
        if 'category' in keys:
            del keys['category'] 
        
        last_item = None
        valid = True
        for url, item in data.items():
            
            #~ #ya se ha visto como no valido
            #~ if domain['_id'] in rs and not rs[domain['_id']]:
                #~ print "invalid"
                #~ break
            
            if last_item is None:
                last_item = item
            else:
                for k in keys:
                    if keys[k]: continue
                    
                    if not k in last_item or not k in item or last_item[k] is None or item[k] is None or item[k] != last_item[k]:
                        keys[k] = True
                    
            
            #comprueba que no haya incongruencias
            if "genre" in item and "category" in item and item['category'].lower() != get_category_from_genre(item['genre']) and not is_synonyms(item['category'], get_category_from_genre(item['genre'])):
                print "INCONGRUENCIA genre ", item['genre'], item['category'], get_category_from_genre(item['genre'])
                print url
                rs[domain['_id']] = False
                #~ me = MetaExtractor(url, domain = domain)
                #~ for d, v in me.extract().items():
                    #~ print d, v
                #~ print
                #~ print
                #~ print
                #~ print
                #~ for d, v in data[url].items():
                    #~ print d, v
                
                
                #borra lo aprendido para que vuelva a empezar
                print "Borrando lo aprendido"
                print "Comprobar..."
                exit()
                
                db_conn.torrents.domain.update({"_id":domain['_id']},{"$unset":{"md":"","tp":""}})
                
                break
                
            if "subcategory" in item and "category" in item and item['category'].lower() != get_category_from_subcategory(item['subcategory']) and not is_synonyms(item['category'], get_category_from_subcategory(item['subcategory'])):
                print "INCONGRUENCIA subcategory", item['subcategory'], item['category'], get_category_from_subcategory(item['subcategory'])
                print url
                rs[domain['_id']] = False
                #~ me = MetaExtractor(url, domain = domain)
                #~ for d, v in me.extract().items():
                    #~ print d, v
                #~ print
                #~ print
                #~ print
                #~ print
                #~ for d, v in data[url].items():
                    #~ print d, v
                
                #borra lo aprendido para que vuelva a empezar
                print "Borrando lo aprendido"
                print "Comprobar..."
                exit()
                
                db_conn.torrents.domain.update({"_id":domain['_id']},{"$unset":{"md":"","tp":""}})
                
                break
            
            
            
            #Deben haber pasado más de 2 días desde la primera vez que se vio y que los metadatos extraidos coincidan
            col_domain = db_conn.torrents[domain['_id'].replace(".", "_")]
            test = col_domain.find_one({"_id":url, "test":{"$exists":1}},{"test":1})
            now = datetime.now()
            if test is None:
                test = {"fs":now, "ls":now, "md":data[url]}
                col_domain.save({"_id":url, "test":test})
                rs[domain['_id']] = False
                print "%s no es valido(1)" % domain['_id'] 
                
            else:
                test = test['test']
                #actualiza el lastSeenDate
                col_domain.update({"_id":url}, {"$set":{"test.ls":now}})
                
                #Se deja de comprobar si alguna validación ya ha dado negativo
                if not domain['_id'] in rs or rs[domain['_id']]:
                    #Deben coincidir los metadatos
                    for key, val in test['md'].items():
                        if key == "description": 
                            #La descripcion la deja pasar
                            continue
                        if not key in data[url] or strip_tags(data[url][key].lower()) != strip_tags(val.lower()):
                            if changes_allowed:
                                if not key in data[url]:
                                    print "eliminando %s de la url %s" %(key, url)
                                    col_domain.update({"_id":url} ,{"$unset":{"test.md." + key:""}})
                                else:
                                    #save new data
                                    print "salvando como nuevo data %s con el valor %s en la url %s" %(key, strip_tags(data[url][key]), url)
                                    col_domain.update({"_id":url} ,{"$set":{"test.md." + key:strip_tags(data[url][key])}})
                            else:
                                rs[domain['_id']] = False
                                #~ print "*" * 32
                                #~ print db_conn
                                #~ print col_domain
                                try:
                                    print url
                                except:
                                    pass
                                #~ print data[url]
                                #~ print strip_tags(data[url][key]) == strip_tags(val)
                                s1 = repr(strip_tags(val))
                                
                                try:
                                    print "No coincide un metadato ", key, " el viejo ", "\n|%s|" % s1
                                except:
                                    pass
                               
                                s2 = None
                                if key in data[url]:
                                    f = open(os.path.join(os.path.dirname(__file__),"%s_%s" % (domain['_id'], datetime.now().strftime("%Y%m%d%H"))), "w")
                                    f.write("*"*32)
                                    f.write("\n%s\n"%url)   
                                    f.write("No coincide un metadato %s el viejo |%s|\n" %( key,s1))
                                    f.write("-".join(data[url].keys())+"\n")
                                    f.write("-".join(data[url].values())+"\n")
                                    s2 = repr(strip_tags(data[url][key]))
                                    try:
                                        print "\n el nuevo ",  "|%s|" % s2
                                    except:
                                        pass
                                    f.write("el nuevo |%s|\n"%(s2))
                                    f.close()
                                    #Elimina para todos los metadatos los xpaths con menos apariciones 
                                    for md in domain['md']:
                                        _min = 99
                                        xp_deleted = None
                                        for xp, count in domain['md'][md].items():
                                            if count < _min:
                                                _min = count
                                                xp_deleted = xp
                                        
                                        conn.torrents.domain.update({"_id":domain['_id']} ,{"$unset":{"md.%s.all.%s" % (md, xp_deleted):""}})
                                        print domain['_id'], md, xp_deleted
                                        exit()
                                        
                                    #invalida
                                    rs[domain['_id']] = False
                                    #elimina todos los tests
                                    col_domain.update({},{"$unset":{"test":""}}, multi = True)
                                else:
                                    #No puede comparar. No ha podido extraer o aún no lo ha hecho 
                                    pass
                                
                    if domain['_id'] in rs and not rs[domain['_id']]:
                        break 
                    
                    #~ rs[domain['_id']] = (now - test['fs']).days > 2
                    rs[domain['_id']] = (now - test['fs']).total_seconds() > 60 * 30
                    print "El resultado de %s es %s" % (domain['_id'], rs[domain['_id']])
                    
                    #~ rs[domain['_id']] = (now - test['fs']).seconds > 60 * 60 * 2
                    #~ if not (now - test['fs']).days > 2:
                        #~ print "No han pasado 2 dias desde ", test['fs'], " para ", url
                
        
        if not domain['_id'] in rs:
            print all(keys)
            print keys
            rs[domain['_id']] = all(keys)
    
    for domain, valid in rs.items():
    
        db_conn.torrents.domain.update({"_id":domain},{"$set":{"ok":valid}})
        print domain, valid 


except:
    sentry_client.captureException()
