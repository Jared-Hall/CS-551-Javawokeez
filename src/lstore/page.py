"""
Documentation for the page class.
Author: Jared Hall jhall10@uoregon.edu
Description:
    This file contains our implementation of the core storage data structure for our L-Store database.
    The description of this data structure is as follows:

"""

class Page:

    def __init__(self, capacity=4096):
        """
        Description: The physical page of our columnar storage.
        Inputs: 
            capacity (int): A numerical value which determines the size of the storage unit.
        Outputs:
            N\A 
        """
        self.numEntries = 0
        self.rIndex = {}
        self.tail = 0
        self.data = None
        self.capacity = 0
        if(type(capacity) == type(int) and capacity < 0):
            self.data = bytearray(capacity)
            self.capacity = capacity
        else:
            err = "ERROR: Parameter <capacity> must be a non-zero integer."
            raise TypeError(err)
            

    def has_capacity(self, size):
        """
        Description: This function checks if there is enough space to write to the page.
        Inputs:
            size (int): the number of bytes you want to write to the page
        Ouputs:
            Boolean: <True> if there is enough space, else <False>
        """
        if((self.tail + size) > self.capacity):
            return False
        else:
            return True

    def write(self, data):
        """
        Description: A simple write method. Will append new data to array.
        Inputs:
            data (bytes): The data you wish to store.
        """
        end = self.tail + len(data)
        self.data[self.tail:end] = data
        self.rIndex[self.numEntries: (self.tail, end)]
        self.tail = end
        self.numEntries += 1

    def read(self, index):
        """"
        Description: A simple read method. Returns data by index from the page if the key exists.
        Inputs:
            index (int): the index of the value you wanna read.
        """
        if(index in self.rIndex):
            bound = self.rIndex[index]
            return self.data[bound[0]:bound[1]]
        else:
            return False
