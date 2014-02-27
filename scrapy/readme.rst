==========
 Contents
==========

* ``torrents`` - the main directory, with all the scrapy spiders and
  several utilities for launching them, testing and adding sources to
  the databases.

* ``tools`` - a library (``smon_utils.py``) and some utilities to
  check and manage from the command line the status of the running
  spiders. Also, a daemon ``scrapy_controller.py`` that checks
  periodically and launches/stops spiders according to the contents of
  the ``scrapy.properties`` configuration file (section
  ``[control]``).

