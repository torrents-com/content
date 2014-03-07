#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Check the status of the scrapy processes and launch/stop them as
necessary.

"""
import logging, os, pymongo, sys, shutil

import smon_utils

from ConfigParser import ConfigParser

def main():
    
    try:
        running_site = {r['spider'].replace("site_",""):r['id'] for r in smon_utils.get_jobs() if not "site_fresh" in r['spider'] and "site_" in r['spider']}
        print running_site
        #launch learned sites
        launch("site", running_site)
        
        exit()
        
        running_site_fresh = {r['spider'].replace("site_fresh_",""):r['id'] for r in smon_utils.get_jobs() if not "site_fresh_deep" in r['spider'] and  "site_fresh" in r['spider']}
        print running_site_fresh
        #launch fresh learned sites
        launch("site_fresh", running_site_fresh, True)
        #launch fresh not learned sites
        launch("site_fresh", running_site_fresh, False)
        running_site_fresh_deep = {r['spider'].replace("site_fresh_deep_",""):r['id'] for r in smon_utils.get_jobs() if "site_fresh_deep" in r['spider']}
        print running_site_fresh_deep
        #launch fresh deep learned sites
        launch("site_fresh_deep", running_site_fresh_deep, True)
        #launch fresh deep not learned sites
        launch("site_fresh_deep", running_site_fresh_deep, False)
        running = [r['spider'] for r in smon_utils.get_jobs() if not "site_" in r['spider']]
        
        
        #launch multisite with all sites not learned
        if not any("multisite" in r['spider'] for r in smon_utils.get_jobs()):
            launch("multisite", [], False)
        
        
        #launch discover
        if not "discover_" in running:
            print "lanzando discover"
            smon_utils.add("discover")
        
        
    except KeyboardInterrupt:
        notifier.stop()
    except Exception as e:
        logging.error('Unhandled exception. I will send an email ' \
                          'and stop now. Exception was: %s' % e)
        raise
        sys.exit(1)
    
    
def launch(spider, running, ok = True):
    """ launch spider using smon_utils """
    
    ds = get_domains()
    
    launch = [i for i in ds if ds[i] == ok and not i.replace(".","_") in running]
    print launch
    start_urls = []
    site_launched = []
    site_last_launched = []
    if spider == "site":
        
        path_launched = os.path.join(os.path.dirname(__file__), "site_running")
        try:
            f = open(path_launched,"r")
            site_last_launched = f.read().split(",")
            f.close()
        except IOError:
            pass
            
    
    
    for l in launch:
        if spider == "multisite":
            start_urls.append("http://%s"%l)
        else:
            print "lanzando %s_%s" %(spider, l)
            
            params = ["start_urls=http://%s"%l]
            if spider == "site":
                
                jobdir = os.path.join(os.path.dirname(__file__),"../torrents/state/%s/"%l)
                
                #si fue lanzado en la anterior elimina estado
                if l in site_last_launched:
                    try:
                        shutil.rmtree(jobdir)
                        print "Estado de %s reiniciado" % l
                    except OSError:
                        pass
                    
                else:
                    site_launched.append(l)
                
                if not os.path.exists(jobdir):
                    os.makedirs(jobdir)
                params.append("setting=JOBDIR=%s"%jobdir)
                
                
            
            
            smon_utils.add(spider, params)
    
    #guarda registro de lo lanza en cada ejecución. 
    #Si tiene que lanzar 2 veces el mismo es porque hay que reiniciar estado
    #Finalizado o algún error
    if spider == "site":
        f = open(path_launched,"w")
        f.write(",".join(site_launched))
        f.close()
    
    
    if ok and spider == "site":
        stop = [i for i in ds if not ds[i] and i.replace(".","_") in running]
        for l in stop:
            print "parando %s_%s" %(spider, l)
            smon_utils.rm("site_%s" % l.replace(".","_"))
            
    
    if spider == "multisite":
        print "lanzando %s_%s" %(spider, ",".join(start_urls))
        smon_utils.add(spider, ["start_urls=%s"%(",".join(start_urls))])

def get_domains():
    """ Return all items of db """
    cfg = ConfigParser()
    cfg.readfp(open(os.path.join(os.path.dirname(__file__),'../torrents/scrapy.properties')))
    
    
    db = pymongo.MongoClient(cfg.get('mongo','host'),int(cfg.get('mongo','port'))).torrents
    return {d['_id']:d['ok'] for d in db.domain.find({"ok":{"$exists":True}},{"_id": True, "ok": True})}


if __name__ == '__main__':
    main()
