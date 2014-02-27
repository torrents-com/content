#!/usr/bin/env python

# This should do a better job than the old/obsolete launch.sh

"""\
Launch all that is necessary to do a "distributed" crawl.
"""

import sys, os, time
import ConfigParser, urllib2, json, MySQLdb
from signal import SIGTERM
from optparse import OptionParser


def main():
    parser = OptionParser(
        version='%prog 0.9',
        usage='%prog [--check-conf] [--test] [--set NAME=VALUE] [--server] ' \
            '<spider> [<spider2> [...]]',
        description=__doc__)
    add = parser.add_option  # write less
    add('--check-conf', action='store_true',
        help='just check the configuration and exit')
    add('--test', action='store_true',
        help='just test that the spider is working fine')
    add('--set', metavar='NAME=VALUE', action='append',
        help='set variable(s) (for example, ' \
            '--set WEBSERVICE_PORT=9000 --set DOWNLOAD_DELAY=2)')
    add('--server', action='store_true',
        help='launch scrapy server and add spider(s) to it')
    add('-a', metavar='NAME=VALUE', action='append',
        help='Set spider argument (may be repeated)')
    opts, rest = parser.parse_args()

    # Fix for the obscure scrapy bug: if no PYTHONPATH is defined, it
    # doesn't add '' to sys.path and may fail to load things like
    # torrents.settings_test
    if 'PYTHONPATH' not in os.environ:
        os.environ['PYTHONPATH'] = ''

    # Try configuration
    #~ if not opts.test and not conf_is_good():
        #~ sys.exit(1)

    if opts.check_conf:
        sys.exit()

    spiders = rest

    # Launch commands and direct output to log files.

    if opts.server:
        steps = [{'command': 'scrapy server',
                  'logfile': 'server.log',
                  'sleep': 2}]
        curl_txt = 'curl http://localhost:16005/schedule.json ' \
            '-d project=default -d spider=%s'
        print red('Warning:'), 'no port is assigned automatically anymore'
        for spider in spiders:
            steps.append( {'command': curl_txt % spider,
                           'logfile': '%s_scrapy.log' % spider} )
    else:
        if len(spiders) != 1:
            sys.exit('One and only one spider can be run in this mode, sorry')
        spider = spiders[0]

        args = ''
        if opts.test:
            os.environ['SCRAPY_SETTINGS_MODULE'] = 'torrents.settings_test'
            args += '--output results_%s.json ' % spider
        if opts.set:
            args += ' '.join('--set '+x for x in opts.set) + ' '
        if opts.a:
            args += ' '.join('-a '+x for x in opts.a) + ' '

        steps = [{'command': 'scrapy crawl %s %s' % (args, spider),
                  'logfile': '%s_scrapy.log' % spider}]
        # Only valid for 1 spider though.

    # In test mode, show output on the screen.
    if opts.test:
        os.system(steps[0]['command'])
        print 'The output line is stored in results_%s.json' % spider
        sys.exit()
    # Uhm, maybe not the best way to do it...

    # Main thing. Launch the different jobs and kill them when
    # finished.
    try:
        pids = [launch(**step) for step in steps]
        while True:
            time.sleep(1000)
        #os.waitpid(pids[-1], 0)
    except KeyboardInterrupt, e:
        print e
        print blue('Continuing...')

    # Clean up
    print blue('Cleaning up...')

    for pid,step in zip(pids, steps)[::-1]:
        try:
            print 'kill %d (%s)' % (pid, step['command'])
            os.kill(pid, SIGTERM)
        except OSError as e:
            print red('Warning:'), 'when trying to kill pid %d : %s' % (pid, e)


# Convenience functions

def color(n, colored=(os.getenv('TERM').startswith('xterm'))):
    return lambda txt: '\033[01;%dm%s\033[00m' % (n, txt) if colored else txt

bold, red, green, yellow, blue, magenta = map(color, [1, 31, 32, 33, 34, 35])


def launch(command, logfile, sleep=0):
    """Launch an external command and log into file."""

    if os.path.exists(logfile):
        print red('Warning:'), 'overwriting', logfile

    pid = os.fork()
    if pid == 0:
        try:
            print green(command)
            print '(see the logs in %s)\n' % blue(logfile)
            os.system('%s > %s 2>&1' % (command, logfile))
        except Exception, e:
            print red('Error:'), e
        os._exit(0)

    time.sleep(sleep)  # leave it some time to start services
    return pid


def conf_is_good(verbose=True):
    """Check if we have a proper configuration.

    It returns True if the config file looks good, proxies work,
    and database responds. False otherwise.

    """
    log = lambda msg: sys.stdout.write(msg) if verbose else None

    cfg = ConfigParser.ConfigParser()
    log('Checking config file (%s)... ' % blue('scrapy.properties'))
    try:
        cfg.readfp(open('scrapy.properties'))

        proxies = cfg.get('remote', 'proxies').split()
        log(green('ok\n'))
    except Exception as e:  # no config file, bad file...
        print red('Error:'), e
        return False

    ok_all = True

    log('Checking proxies (%d total)...\n' % len(proxies))
    for i, proxy in enumerate(proxies):
        log('    %2d / %d %s ' % (i+1, len(proxies), blue(proxy)))
        try:
            h = urllib2.ProxyHandler({'http': proxy})
            urllib2.build_opener(h).open('http://goo.gl/', timeout=4).read()
            log(green('ok\n'))
        except Exception as e:  # connection refused, timeout...
            print red('Error:'), e
            ok_all = False

    log('Checking database connection... ')
    try:
        get = lambda x: cfg.get('database', x)  # get things from db section
        db = MySQLdb.connect(get('host'), user=get('user'),
                             passwd=get('passwd'), db=get('db'))
        db.close()
        log(green('ok\n'))
    except Exception as e:
        print red('Error:'), e
        ok_all = False

    return ok_all



if __name__ == '__main__':
    main()
