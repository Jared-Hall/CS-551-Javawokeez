import logging

from lstore.setupLogging import *

class subClass():
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log = setupLogger(False, "DEBUG", self.log, 8)
        self.log.debug("Hello from init")
        

    def method1(self):
        self.log.debug(f"Hello from method1")

