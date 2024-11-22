import logging

from lstore.setupLogging import *

class subClass2():
    def __init__(self, sub1):
        self.sub1 = sub1
        self.log = logging.getLogger(self.__class__.__name__)
        self.log = setupLogger(False, "DEBUG", self.log, 4)

        self.log.debug("Hello from init")

    def method1(self):
        self.log.debug(f"Hello from method1")
        self.sub1.method1()
