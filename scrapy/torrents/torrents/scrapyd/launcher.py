from scrapyd.launcher import Launcher, ScrapyProcessProtocol

import sys

from scrapy.utils.python import stringify_dict
from scrapyd.utils import get_crawl_args
from scrapyd.interfaces import IEnvironment

from twisted.internet import reactor

class mLauncher(Launcher):
    """ override to included 'args' in protocol """ 
    
    def _spawn_process(self, message, slot):
        msg = stringify_dict(message, keys_only=False)
        project = msg['_project']
        args = [sys.executable, '-m', self.runner, 'crawl']
        args += get_crawl_args(msg)
        e = self.app.getComponent(IEnvironment)
        env = e.get_environment(msg, slot)
        env = stringify_dict(env, keys_only=False)
        pp = mScrapyProcessProtocol(slot, project, msg['_spider'], msg['_job'], env, args)
            
        pp.deferred.addBoth(self._process_finished, slot)
        reactor.spawnProcess(pp, sys.executable, args=args, env=env)
        self.processes[slot] = pp


class mScrapyProcessProtocol(ScrapyProcessProtocol):

    def __init__(self, slot, project, spider, job, env, args):
        self.args = args
        ScrapyProcessProtocol.__init__(self, slot, project, spider, job, env)
