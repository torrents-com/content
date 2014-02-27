#!/usr/bin/env python

from distutils.core import setup

setup(
    name='smon',
    version='0.8',
    description='Scrapy monitorization tools',
    author='Jordi Burguet-Castell',
    author_email='jordi@mp2p.net',
    py_modules=['smon_utils'],
    license='GNU General Public License, version 3',
    scripts=['smon', 'smon-ls', 'smon-add', 'smon-rm'])
