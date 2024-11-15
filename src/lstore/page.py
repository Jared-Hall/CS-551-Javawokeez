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
       iii. All data stored in the page has a fixed chunck size of 
"""
<<<<<<< HEAD
=======
import uuid

>>>>>>> 10dd497085d4d2a784bbcdec4b6f3de49ad55e90
class Page:

    def __init__(self, pid, capacity=4096, size=8, fromFile=False):
        """
        Description: The physical page of our columnar storage. A page contains a single column of data.
        Notes: pid's must be unique since it is both an identifier for the page and it's data file.
               e.g., (page 1)->self.pageID <- "P-106" <- saved in file: "P-106.data"
               The Pags' local index is also saved into a file. File (prev example): "P-106.index"

        Inputs: 
            pid (str): A unique numerical intentifier for this page. Format: "P-<int>"
            capacity (int): A numerical value which determines the size of the storage unit.
            size (int): A numerical value containing the fixed length of all data to be inserted into the column.
            fromFile (bool): A boolean flag that tells the page to load data and index from file when
                             building the page. By default pages are build in-memory unless this is set to True.
        Outputs:
            Page Object
        
        Internal Objects:
            pIndex (dict): A dictionary containing key-value pairs where key=rid, value=data index
            rIndex (list): A list of open data indexes
            [0xBBBB, 0xBBBB]
                 0     1
            {rid1:0, rid2:1, ...} <- page Index
            [1] <- rIndex
        """
        self.numEntries = 0
        self.maxEntries = 4096//size
        self.tail = 0
        self.capacity = 0
        self.entrySize = size
        
        self.data = None
        self.pIndex = {}
        self.rIndex = []
        if(type(pid) != type("str") or "P-" not in pid):
            err = "ERROR: Parameter <pid> must be a string in the format P-<int>."
            raise TypeError(err)
        else:
            self.pageID = pid
            
        if(fromFile):
            self.load()
        elif(type(capacity) == type(1) and capacity > 0):
            self.data = bytearray(capacity)
            self.capacity = capacity
        else:
            err = "ERROR: Parameter <capacity> must be a non-zero integer."
            raise TypeError(err)
<<<<<<< HEAD
=======
        
        self.pageID = uuid.uuid4() # Generate a Unique id
            
>>>>>>> 10dd497085d4d2a784bbcdec4b6f3de49ad55e90

    def has_capacity(self, size):
        """
        Description: This function checks if there is enough space to write to the page.
        Inputs:
            size (int): the number of bytes you want to write to the page
        Ouputs:
            Boolean: <True> if there is enough space, else <False>
        """
        if(self.numEntries < self.maxEntries):
            return True
        else:
            return False
    
    def save(self):
        """
        Description: This method saves the page to disk. 
        Format:
        [<pid>.index]
        rid:idx,rid:idx,... <- this is the page index
        (rid,idx),(rid,idx),... <- this is the remove index
        [data]
        )0xBBBBBBBB0xBBBBBBB...
        """
        #Step-01: Write the page index
        with open("storage/"+self.pid+".index", "w") as idxFile:
            items = self.pIndex.items()
            rep = ""
            for item in items:
                rep += str(item[0])+":"+str(item[1])+","
            rep = rep[:-1]
            idxFile.write(rep+"\n")
            idxFile.write(','.join(self.rIndex)+'\n')
        with open("storage/"+self.pid+".data", "wb") as dataFile:
            dataFile.write(bytes(self.data))

    def load(self):
        """
        Description: This method loads the page (data and index) from disk.
        Expected Format:
        [pIndex]
        rid:idx,rid:idx,...
        [rIndex]
        (idx, idx, idx,...
        [data]
        )0xBBBBBBBB0xBBBBBBB...
        """
        with open("storage/"+self.pid+"index", "r") as idxFile:
            pIndex = idxFile.readline().split(",")
            rIndex = idxFile.readline().split(",")
            self.pIndex = {int(key):int(value) for key,value in (item.split(":") for item in pIndex)}
            self.rIndex = [(int(idx) for idx in rIndex)]
        with open("storage/"+self.pid+".data", "rb") as dataFile:
            self.data = dataFile.read()

    def _encode(self, data):
        if(type(data) == str):
            return data.encode('utf-8')
        else:
            return str(data).encode('utf-8')
    
    def _decode(self, data):
        return data.decode('utf-8')

    def write(self, rid, data):
        """
        Description: A simple write method. Will append new data to array.
        Inputs:
            data (any): The data you wish to store. Any data type is fine since I do the encoding here
        """
        data = self._encode(data)
        pos = -1
        if(len(self.rIndex) > 0):
            pos = self.rIndex.pop()
        else:
            pos = self.tail
            self.tail += 8
        self.data[pos:(pos+8)] = data
        self.pIndex[rid] = pos
        self.numEntries += 1

    def read(self, rid):
        """"
        Description: A simple read method. Returns data by index from the page if the key exists.
        Inputs:
            index (int): the index of the value you wanna read.
        """
        if(rid in self.pIndex):
            idx = self.rIndex[rid]
            return self.data[idx:(idx+8)]
        else:
            return False
        


if(__name__== "__main__"):
    p = Page(20)
    print(p.capacity)
    a = bytearray('Hello', 'utf-8')
    b = bytearray('World', 'utf-8')
    p.write(a)
    print(p.data)
    p.write(b)
    print(p.data)
    print(p.read(0).decode('utf-8'))
    print(p.read(1).decode('utf-8'))

