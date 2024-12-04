from threading import Lock
from collections import defaultdict

class RWLockManager:
    def __init__(self):
        self.locks = {}
    
    def addLock(self, key):
        self.locks[key] = RWLock()

    def hasLock(self, key):
        return key in self.locks
    
    def removeLock(self, key):
        del self.locks[key]

    def acquireRLock(self, key):
        return self.locks[key].acquire_r()

    def releaseRLock(self, key):
        return self.locks[key].release_r()
    
    def acquireWLock(self, key):
        return self.locks[key].acquire_w()
    
    def releaseWLock(self, key):
        return self.locks[key].release_w()


class RWLock:
    def __init__(self):
        self.lock = Lock()
        self._activeReaders = 0
        self._activeWriters = False

    def acquire_r(self):
        if(self._activeWriters == False):
            self.lock.acquire()
            self._activeReaders += 1
            return True
        else:
            return False

    def release_r(self):
        if(self._activeReaders > 1):
            self._activeReaders -= 1
            return True
        elif(self._activeReaders == 1):
            self._activeReaders = 0
            return True
        else:
            return False


    def acquire_w(self):
        if(self._activeReaders > 0):
            return False
        elif(self._activeWriters): 
            return False
        else:   
            self._activeWriters = True
            self.lock.acquire()
            return True

    def release_w(self):
        """ Release a write lock. """
        if(self._activeWriters):
            self._activeWriters = False
            self.lock.release()
            return True
        else:
            return False
