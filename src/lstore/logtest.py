import logging

from lstore.setupLogging import *
from lstore.subclass1 import *
from lstore.subclass2 import *

class mainClass():
    def __init__(self, mode=True):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log = setupLogger(False, "DEBUG", self.log, 0)
        
        self.log.debug(f"Hello from init! Building subclasses...")
        self.sub1 = subClass()
        self.sub2 = subClass2(self.sub1)
        self.log.debug(f"Complete!")

        

    def method1(self):
        self.log.debug(f"Hello from method1! Calling subclass.method 1 directly...")
        self.sub1.method1()
        self.log.debug(f"Complete! Calling subclass2.method1 directly...")
        self.sub2.method1()
        self.log.debug("Complete!")

    def method2(self):
        self.log.debug(f"Hello from method2! Calling the subclasses indirectly...")
        self.method1()
        self.log.debug("Complete!")

