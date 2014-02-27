==========
 Crawlers
==========

Scrapy crawlers

See scrapy documentation at
http://doc.scrapy.org/en/latest/intro/tutorial.html


Run
===

Run with::

  ./launch.py <spider>

where ``<spider>`` is one of ``/torrents/spiders``.
It will set all the necessary running services and finally call ``scrapy
crawl <spider>``.

If it fails, most likely there is some process using the port of the
Pyro naming server. Quick fix::

  fuser -k 9090/tcp

(If running in FreeBSD it may not work. Just find the program that is
listening on port 9090 with ``sockstat -4 | grep 9090`` and kill it.)


Server Run
==========

The easy way::

  ./launch.py --server <spider1> <spider2> ...

It will launch the scrapy server and a job for each spider specified.

It is equivalente to the following. Run the spiders using the scrapy
server::

  scrapy server

It will listen in port 16005 (as stated in the ``scrapyd.conf`` file).

Then, add a spider, so something like::

  curl http://localhost:16005/schedule.json -d project=default -d spider=site -d setting=WEBSERVICE_PORT=9009


All this stuff is described here:
http://doc.scrapy.org/en/latest/topics/scrapyd.html


Configuration
=============

There should be a symbolic link ``scrapy.properties`` to a file with
the same name in the glocal directory ``config/`` where we store the
settings for all the crawlers.

That file has to be outside of the repository as we have all the
information for connecting to the database in there.


Logs
====

There are different kind of log files produced:

* ``<spider>.<host>.log`` - the most important one, contains info that
  will be parsed later and go into the mongos (hopefully). 
* ``scrapy.log`` - information from the scrapy process

* ``<spider>_spider.log`` - information from the spider (it runs as a
  separate process and communicates thru Pyro4)

* ``http_proxy.log`` - information from the proxy server (if running
  locally)

* ``naming_server.log`` - information from the Pyro4 naming server
  (used to communicate the spider with scrapy)


Notes
=====

After failing to have two separate spiders to be called when
appropriate, this version just has a big spider that dispatches when
appropriate.

To change the order of scrapping, to breath-first, sometimes I put
this in ``settings.py``::

  DEPTH_PRIORITY = 1
  SCHEDULER_DISK_QUEUE = 'scrapy.squeue.PickleFifoDiskQueue'
  SCHEDULER_MEMORY_QUEUE = 'scrapy.squeue.FifoMemoryQueue'
  # See
  # http://doc.scrapy.org/en/latest/faq.html#does-scrapy-crawl-in-breath-first-or-depth-first-order

Added a random factor in the priority of the requests, so I can see
different results each time I run it.
