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
        bufferPool = BufferPool()
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

    def __init__(self):
        """
        Description: The BufferPool Manager for the Database
        Inputs: 
            N/A
        Outputs:
            N\A 
        """
        self.pageDirectoryCapacity = 10000000

        self.pageIds = [] # Keeps track of all Page ids
        self.pageDirectory = {} # Maps Page Ids -> Pages
        pass

    def createNewPageAndGetId(self):
        """
        Description: This function creates a new page and returns the id of the page

        Inputs: 
            N/A
        Outputs:
            Returns the page ID of the created page
        """
        page = Page()

        if len(self.pageDirectory.keys()) <= self.pageDirectoryCapacity:
            self.pageDirectory[page.pageID] = page
        else:
            # TODO: EVICT a page from directory and add a new one
            pass
        # self.pageIds.append(pageId)
        return page.pageID
    
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

