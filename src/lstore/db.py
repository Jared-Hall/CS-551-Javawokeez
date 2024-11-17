"""
Documentation for the db(database) class.
Author: Nabil Abdel-Rahman nabilabdel-rahman@outlook.com, Jack O'Donnell Jodonnel@uoregon.edu 
Description:
    This file contains the implementation for the database. 
    The database handles creating, deleting, and retrieving tables 

"""

from lstore.table import Table
from lstore.page import Page


class Database():

    def __init__(self):
        self.tables = {} #Store name and tables as key:value
        self.path = None 
        pass

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
        pass

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
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        bufferPool = BufferPool(num_columns)
        table = Table(name, num_columns, key_index, bufferPool)
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

        if name in self.tables: 
            return self.tables[name]
        else:
            raise Exception("Table not in database")
        pass


class BufferPool:

    def __init__(self, num_columns):
        """
        Description: The BufferPool Manager for the Database
        Inputs: 
            N/A
        Outputs:
            N\A 
        """
#         Table 5 columns 3*pageMax Indexes (5*PageMax pages)
        # {
        #     col1-Mem(Full): [<PageRef3>,     <PageRef4>]
        #                          Full (1.2)   FULL (LFU: 0.001)
            
        #     col1-Mem(Partial):  [<PageRef5>]
        #                           n/2 (LFU: 0.0000001)

        #     col1-Disk(Full): ["PID1", "PID2", ...]
        #                        Full   Full
            
        #     col-Disk(partial): [("PID7", 510), .... ("PID10", 236)]
        #                                 cap
        # } 
        # Test value


        self.pageDirectoryCapacity = 10000000

        # self.pageIds = [] # Keeps track of all Page ids
        # self.pageDirectory = {} # Maps Page Id -> Page

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
        pass

    def hasCapacity(self):
        """
        Checks if Bufferpool has maximum ampunt of ful pages possible
        """

        pass

    def hasCapacityForColumn(self, index):
        if len(self.colMemFull(index)) < self.maxColumnCapacity:
            return True
        return False        

    def createNewPageAndGetIdForColumn(self, index):
        """
        Description: This function creates a new page and returns the id of the page

        Inputs: 
            N/A
        Outputs:
            Returns the page ID of the created page
        """
        page = Page("P-" + str(self.pageCount))
        
        self.colMemPartial[index].append(page)
        # self.
        return page.pageID
    
    def getPageFromDisk(self, pageID):
        page = Page(pageID)
        if not page.load():
            raise FileExistsError
        return page
    

    def evict(self):
        # TODO: evict a page from pageDirectory        
        pass
    
    # def getPageById(self, pageId):
    #     """
    #     Description: This function returns the page with the page ID

    #     Inputs: 
    #         N/A
    #     Outputs:
    #         Returns the page ID of the created page
    #     """

    #     if pageId not in self.pageDirectory:
    #         # TODO: pass id onto the page class and try to load from disk
    #         # TODO: If found in disk return page
    #         # TODO: Else return an error
    #         pass
    #     return self.pageDirectory[pageId]        
