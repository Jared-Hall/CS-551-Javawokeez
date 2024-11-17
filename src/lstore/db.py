"""
Documentation for the db(database) class.
Author: Nabil Abdel-Rahman nabilabdel-rahman@outlook.com, Jack O'Donnell Jodonnel@uoregon.edu 
Description:
    This file contains the implementation for the database. 
    The database handles creating, deleting, and retrieving tables 

(m2 notes) 
Bufferpool Memory Management: Column -> Pages 
Description: The Buffer Pool (BP) holds references to every page in the DB, both in memory and on disc.
                The BP index is a 2-stage index: 
                1st Stage: [Column1_Index, Column2_index, ...]
                2nd Stage: column-specific index
                Each column index has the following format:
                [
                    [<PageRef>, ...], # In-memory (Empty/Partially filled pages)
                    [<PageRef>, ...], # In-memory (Full pages)
                    [<PID>, ...], # On disk (Empty/Partially filled pages)
                    [<PID>, ...], # On disk (Full pages)
                ]
Format:
[
    [
        [<PageRef>, ...],
        [<PageRef>, ...],
        [<PID>, ...],
        [<PID>, ...]
    ], 
    ...
]

"""

from lstore.table import Table
from lstore.page import Page
from lstore.index import Index


class Database():

    def __init__(self):
        self.tables = {} #Store name and tables as key:value
        self.path = None 

    # Not required for milestone1
    def open(self, path):
        """
        path is a string reference to a folder name 
        the path stores all the pages for that database 
        we need some object to store data about the database on disk 
        data included: tables
        table data: colDisk needs to be saved so we know which pages in disk are partial and full when reloaded 
        table data: key_rid locations map needs to be loaded 
        
        
        """

        self.path = path

    def close(self):
        """
        db can only be closed when all pages in memory are unpinned 
        all data is written to disk and colDisk indexes are updated to contain all pages
        save the colDisk indexes to disk to be used on reload 
        save the key_rid index to disk 
        
        """
        for name, table in self.tables:
            buffer = table.bufferpool
            with open(str(self.path)+"/"+str(name)+"-full.bin", "wb") as file:
                file.write((table.key).to_bytes(4, 'big'))
                file.write((table.num_columns).to_bytes(4, 'big'))
                for col in buffer.colDiskFull:
                    file.write(len(col).to_bytes(4, 'big')) 
                    for PID in col: 
                        file.write(len(PID).to_bytes(4, 'big'))
                        file.write(PID.encode("utf-8"))

            with open(str(self.path)+"/"+str(name)+"-"+str(table.num_columns)+"-"+str(table.key)+"-partial.bin", "wb") as file:
                file.write((table.key).to_bytes(4, 'big'))
                file.write((table.num_columns).to_bytes(4, 'big'))
                for col in buffer.colDiskPartial:
                    file.write(len(col).to_bytes(4, 'big')) 
                    for PID in col: 
                        file.write(len(PID).to_bytes(4, 'big'))
                        file.write(PID.encode("utf-8"))

                
        self.path = None 

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        bufferPool = BufferPool(num_columns, index)
        index = Index(num_columns)
        table = Table(name, num_columns, key_index, bufferPool, index)
        self.tables[name] = table #Store the table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables: 
            del self.tables[name]
        else:
             raise Exception("Table not in database ")
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        diskFull = []
        diskPartial = []
        with open(str(self.path)+"/"+name+"-full.bin", "rb") as file:
            key = int.from_bytes(file.read(4), 'big') 
            numCols = int.from_bytes(file.read(4), 'big') 
            for i in range(numCols):
                col = []
                numPages = int.from_bytes(file.read(4), 'big') 
                for j in range(numPages): 
                    PID_len = int.from_bytes(file.read(4), 'big') 
                    data = file.read(PID_len).decode('utf-8')
                    col.append(data)
                diskFull.append(col)

        with open(str(self.path)+"/"+name+"-full.bin", "rb") as file:
            key = int.from_bytes(file.read(4), 'big') 
            numCols = int.from_bytes(file.read(4), 'big') 
            for i in range(numCols):
                col = []
                numPages = int.from_bytes(file.read(4), 'big') 
                for j in range(numPages): 
                    PID_len = int.from_bytes(file.read(4), 'big') 
                    data = file.read(PID_len).decode('utf-8')
                    col.append(data)
                diskPartial.append(col) 

        table = self.create_table(name, numCols, key)
        table.bufferpool.colDiskFull = diskFull 
        table.bufferpool.colDiskPartial = diskPartial
        self.tables[name] = table
        return table 


class BufferPool:

    def __init__(self, num_columns, path):
        """
        Description: The BufferPool Manager for the Database
        Inputs: 
            N/A
        Outputs:
            N\A 
        """

        self.pageDirectoryCapacity = 10000000
        self.pageDirectory = {}
        self.path = path

        # col-mem-partial & col-mem-full make up the complete buffer(memory) pool of Page objects
        self.colMemPartial = [[]] * num_columns # Every element is an array that stores Page objects at the corresponding column index
        self.colMemFull = [[]] * num_columns # Every element is an array that stores Page objects at the corresponding column index

        # col-disk-partial & col-disk-full make up the pageIds of all the pages stored on disk 
        self.colDiskPartial = [[]] * num_columns # Every element is an array that stores Page ids at the corresponding column index
        self.colDiskFull = [[]] * num_columns # Every element is an array that stores Page ids at the corresponding column index

        self.maxColumnCapacity = 10

        self.pageCount = 0
        self.dirtyPageList = [None] * self.pageDirectoryCapacity
        self.dirtyPageListTail = 0

    def hasCapacityForColumn(self, index):
        if len(self.colMemFull(index)) < self.maxColumnCapacity:
            return True
        return False        

    def createPage(self, index):
        """
        Description: This function creates a new page and returns the id of the page

        Inputs: 
            N/A
        Outputs:
            Returns the page ID of the created page
        """
        PID = "P-" + str(self.pageCount)
        page = Page(PID)
        self.colMemPartial[index].append(page)
        self.pageDirectory[PID] = page
        self.pageCount += 1
        return PID
    
    def loadPage(self, pid):
        if(pid in self.pageDirectory):
            idx = self.pageDirectory[pid] #(isFull, colIdx, index in BP)
        page = Page(pid)
        if(isFull):
            status = page.load(self.path, "-full")
            self.colMemFull[column].append(page)
        else:
            status = page.load(self.path, "-partial")
            self.colMemPartial[column].append(page)
        if(status):
            raise FileExistsError("Page: "+pageID+"Does not exist.")
        return page
    
    def savePage(self, pid):
        """
        Description: This method writes the page to disk and removes it from the active bufferpool.
        Inputs:
            pageID(str): The ID of the page we want to write to disk.
        """
        if(pid in self.pageDirectory):
            idx = self.pageDirectory[pid] #(isFull, colIdx, index in BP)
            if(idx[0]): #full page
                page = self.colMemFull[idx[1]].pop(idx[2])
                page.save(self.path, "-full")
                index = (True, idx[1], len(self.colDiskFull))
                self.colDiskFull[idx[1]].append(page.pageID)
                self.pageDirectory[pid] = index
            else:
                page = self.colMemPartial[idx[1]].pop(idx[2])
                page.save(self.path, "-partial")
                index = (False, idx[1], len(self.colDiskPartial))
                self.colDiskPartial[idx[1]].append(page.pageID)
                self.pageDirectory[pid] = index
            return True
        return False

    def evict(self):
        # TODO: evict a page from pageDirectory        
        pass       
