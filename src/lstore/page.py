import time
import os
"""
Documentation for the page class.
Author: Jared Hall jhall10@uoregon.edu
Description:
    This file contains our implementation of the core storage data structure for our L-Store database.
    The page class contains all of the necessary operations for the page. See the method documentation
    for a detailed breakdown of all methods.
Modifications:
    ~ M2 ~
    1. I changed the page class to add efficient bufferpool maniputation to the page.
       The page class should be able to handle all operations regarding the page.
       i. I added a UID to the page for page-based indexing.
       ii. I added methods to read from and write to a file as well as a method for pages to 
           automatically do so.
       iii. All data stored in the page has a fixed chunck size of 8 bytes
"""
class Page:

    def __init__(self, pid, path, capacity=4096, size=8):
        """
        Description: The physical page of our columnar storage. A page contains a single column of data.
        Notes: pid's must be unique since it is both an identifier for the page and it's data file.
               e.g., (page 1)->self.pageID <- "P-106" <- saved in file: "P-106.data"
               The Pags' local index is also saved into a file. File (prev example): "P-106.index"

        Inputs: 
            pid (str): A unique numerical intentifier for this page. Format: "P-<int>"
            capacity (int): A numerical value which determines the size of the storage unit.
            size (int): A numerical value containing the fixed length of all data to be inserted into the column.

        Outputs:
            Page Object
        
        Internal Objects:
            pageIndex (dict): A dictionary containing a page-wise index of values with their absolute version
            rIndex (list): A list of open data indexes
            dTail (int): The last open position in the data array
            rTail (int): the last open position in the rIndex array
            data (ByteArray): The actual data of the column in bytes
            maxEntries (int): The maximum number of entries (max = capacity//entrySize)
            entrySize (int): The fixed size of each entry.
            pin (int): A variable that contains the number of active transactions on this page.   
            pageIndex (dict): A dictionary version based value-key 2nd level Index (m1: Bonus)
            
            rIndex (list): A list of open indecees in the data array (m2 Bonus: efficient compactless storage).
            Format:
                            [idx_1, idx_2, ..., idx_k]
            LFU (float): (m2 Bonus: Eviction Policy) This variable contains a measurement of the "rate of use" that a page sees.
                         The rate of use is calculated by taking the number of times a page has been read from or written to
                         in a fixed cycle and dividing this by a fixed time window.  
        """
        self.LFU = 0
        self.pin = -1
        self.isDirty = False

        self.startTime = time.time()
        self.cycle = 30

        self.capacity = 0
        self.path = path
        self.data = None
        self.availableOffsets = None
        
        if(type(pid) != type("str") or "P-" not in pid):
            err = "ERROR: Parameter <pid> must be a string in the format P-<int>."
            raise TypeError(err)
        else:
            self.pageID = pid
            
        if(type(capacity) == type(1) and capacity > 0):
            self.data = bytearray(capacity)
            self.capacity = capacity
            self.maxEntries = capacity//size
            self.availableOffsets = [x for x in range(self.maxEntries)]
        else:
            err = "ERROR: Parameter <capacity> must be a non-zero integer."
            raise TypeError(err)
    
    def setDirty(self):
        self.isDirty = True
    
    def setClean(self):
        self.isDirty = False

    def hasCapacity(self):
        """
        Description: This function checks if there is enough space to write to the page.
        Inputs:
            size (int): the number of bytes you want to write to the page
        Ouputs:
            Boolean: <True> if there is enough space, else <False>
        """
        return True if(len(self.availableOffsets) > 0) else False
    
    def save(self, suffix):
        """
        Description: This method saves the page data and it's available offsets to disk.
        """
        status = True
        #Step-01: Write the page index
        path = self.path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(f"{self.path}{self.pageID}{suffix}.offsets", "w") as offsetFile:
            offsetFile.write(','.join([str(x) for x in self.availableOffsets]))
        with open(f"{self.path}{self.pageID}{suffix}.bin", "wb") as dataFile:
            dataFile.write(bytes(self.data))
        return status

    def load(self, suffix):
        """
        Description: This method loads the page (data and index) from disk.
        """
        status = True
        with open(f"{self.path}{self.pageID}{suffix}.bin", "rb") as dataFile:
            self.data = bytearray(dataFile.read())
        with open(f"{self.path}{self.pageID}{suffix}.offsets", "r") as offsetFile:
            self.availableOffsets = [int(x) for x in offsetFile.read().split(',')]
        return status

    def write(self, value):
        """
        Description: A simple write method. Will append new data to array.
        Inputs:
            value (any): The data value to be stored. Will be encoded as a string.
        Outputs:
            index (int): The integer index that the data was stored at.
        """
        self.LFU += 1
        index = self.availableOffsets.pop()
        self.data[index : (index + 8)] = str(value).encode('utf-8')
        return index
        
    def read(self, index):
        """"
        Description: A simple read method. Returns data by index from the page if the key exists.
        Inputs:
            index (int): the index of the value you wanna read.
        """
        self.LFU += 1
        return (self.data[index:(index+8)]).decode('utf-8')

    def remove(self, index):
        """
        Description: Removes data in the page from the given index.
        Inputs:
            index (int): the index of the value you wanna delete.
        """
        self.LFU += 1
        self.availableOffsets.append(index)
    
    def calculateLFU(self):
        """
        Description: Removes data in the page from the given index.
        Inputs:
            index (int): the index of the value you wanna delete.
        """
        endTime = time.time()
        LFU = self.LFU/((self.startTime - endTime) % self.cycle)
        self.LFU = 0
        return LFU