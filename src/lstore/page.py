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

    def __init__(self, pid, capacity=4096, size=8):
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
        self.numEntries = 0
        self.dTail = 0
        self.rTail = 0
        self.capacity = 0
        self.entrySize = size
        self.data = None
        self.LFU = 0
        self.pin = -1
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

    def has_capacity(self):
        """
        Description: This function checks if there is enough space to write to the page.
        Inputs:
            size (int): the number of bytes you want to write to the page
        Ouputs:
            Boolean: <True> if there is enough space, else <False>
        """
        if(len(self.availableOffsets) > 0):
            return True
        else:
            return False
    
    def save(self):
        """
        Description: This method saves the page data and it's available offsets to disk.
        """
        status = True
        #Step-01: Write the page index
        with open("storage/"+self.pageID+".offsets", "wb") as offsetFile:
            offsetFile.write(','.join(self.availableOffsets))

        with open("storage/"+self.pageID+".bin", "wb") as dataFile:
            dataFile.write(bytes(self.data))
        return status

    def load(self):
        """
        Description: This method loads the page (data and index) from disk.
        Data structures:
        pageIndex:
        {
            <value>: {
                        <version> : [<key>, ...],
                        ...
                    },
                    ...
        }

        removeIndex: 
        [idx, idx, idx, idx, ...]

        Format (PID.index):
        Page Index:
        <value>:<version1>#<key1>#<key2>#<key3>#...#<keyn>|<version2>#...#,<value2>:...
        Remove Index:
        idx,idx,idx ,... <- this is the remove index
        
        Format(PID.bin): 
        0xBBBBBBBB, 0xBBBBBBB...
        """
        status = True
        with open("storage/"+self.pid+".bin", "rb") as dataFile:
            self.data = bytearray(dataFile.read())
        
        return status

    def _encode(self, data):
        if(type(data) == str):
            return data.encode('utf-8')
        else:
            return str(data).encode('utf-8')
    
    def _decode(self, data):
        return data.decode('utf-8')

    def _insertData(self, data):
        """
        Description: This method inserts the data and returns the index
        Inputs:
            data (bytes): This variable contains a bytes encoded string.  
        """
        rawData = self._encode(str(value))
        index = -1
        if(len(self.availableOffsets) > 0):
            index = self.rIndex.pop()
        else:
            index = self.tail
            self.tail += 8
        self.data[index:(index+8)] = rawData


    def write(self, value):
        """
        Description: A simple write method. Will append new data to array.
        Inputs:
            key (any): The primary key for the data. Will be encoded as a string.
            value (any): The data value to be stored. Will be encoded as a string.
            version (int): The absolute version of the data. 0 means it belongs to a BR, all else is TR data
        Outputs:
            index (int): The integer index that the data was stored at.
        """
        rawData = self._encode(str(value))
        index = -1
        if(len(self.availableOffsets) > 0):
            index = self.availableOffsets.pop()
        else:
            index = self.tail
            self.tail += 8
        self.data[index:(index+8)] = rawData
        return index
        

    def read(self, index):
        """"
        Description: A simple read method. Returns data by index from the page if the key exists.
        Inputs:
            index (int): the index of the value you wanna read.
        """
        return self.data[index:(index+8)]
        


if(__name__== "__main__"):
    p1 = Page('P-1')
    print("Page created (capacity):", p1.capacity)
    data = []
    for version in [0,1,2,3]:
        for key in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
            value = key+10+version
            data.append((key, value, version))
    
    index = {}
    for item in data:
        index[item[0]] = p1.write(item[2])
        print("Inserted: ", data, " - @ index:", index[item[0]])
    
    for item in data:
        print("Reading data for key("+str(item[0])+"): ", p1.read(index[item[0]]))
    
    print("===Save Test===")
    print("Status: ", p1.save())
    print("===============")

    print("===Load Test===")
    p2 = Page(p1.pageID)
    st = p2.load()
    print("Status: ", st)
    print("Correct: ", p2.data == p1.data, p2.availableOffsets == p1.availableOffsets)

