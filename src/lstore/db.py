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
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
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

        # if len(self.pageDirectory[index][self.COLL_MEM_FULL_KEY]) + len(self.pageDirectory[index][self.COLL_MEM_PARTIAL_KEY]) > self.maxColumnCapacity:
        #     self.evict()
        
        # page = Page("P-" + str(self.pageCount))
        # self.pageDirectory[index][self.COLL_MEM_PARTIAL_KEY].append(page)
        # self.pageCount += 1
        # return page.pageID
    
    # def createNewPageAndGetId(self):
    #     """
    #     Description: This function creates a new page and returns the id of the page

    #     Inputs: 
    #         N/A
    #     Outputs:
    #         Returns the page ID of the created page
    #     """
    #     page_index = len(self.pageDirectory.keys()) + 1
    #     page = Page("P-" + str(page_index))

    #     if len(self.pageDirectory.keys()) > self.pageDirectoryCapacity:
    #         self.evict()
    #     self.pageDirectory[page.pageID] = page
    #     self.dirtyPageList[self.dirtyPageList] = page

    #     self.dirtyPageList += 1

    #     return page.pageID
    
    def evict(self):
        # TODO: evict a page from pageDirectory        
        pass
    
    def getPageById(self, pageId):
        """
        Description: This function returns the page with the page ID

        Inputs: 
            N/A
        Outputs:
            Returns the page ID of the created page
        """

        if pageId not in self.pageDirectory:
            # TODO: pass id onto the page class and try to load from disk
            # TODO: If found in disk return page
            # TODO: Else return an error
            pass
        return self.pageDirectory[pageId]        

    def getPage(self):
        pass

    def evictPage(self):
        pass
