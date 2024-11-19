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
from os import remove


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
        #For each table
        #print("TABLES: ", self.tables)
        for name, table in self.tables.items():
            #Step-01: Write all memory to disk and close the table
            table.save()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        bufferpool = BufferPool(num_columns, 10*num_columns)
        table = Table(name, num_columns, key_index, bufferpool, self.path)
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
        numColumns = 0
        with open(f"{self.path}/{name}/{name}.meta", "r") as file:
            numColumns = int(file.readline().strip('\n'))
            key = int(file.readline())
        table = self.create_table(name, numColumns, key)
        table.load()
        self.tables[name] = table
        return table 


class BufferPool:

    def __init__(self, num_columns, maxPages=10):
        """
        Description: The BufferPool Manager for the Database
        Inputs: 
            num_columns (int): The number of columns for this table.
            path (str): The path variable to the location on disk for storing files.
            maxPages (int): The maximum number of pages that can be stored in memory for each column.
        Outputs:
            N\A 
        
        Internal Objects:
            pageDirectory (dict): Maps Page IDs to metadata about that page. 
                                  Format: {<pid> : (isFull, inMem, columnIdx, pageIdx)}
            path (str) : The path for the bufferpool. Composed of: dbDir/tableDir/
            colMemPartial (lst): [ColumnIndex_1, ColumnIndex_1, ...], col1_index: [<PageRef>, ...]
            colMemFull (lst): [ColumnIndex_1, ColumnIndex_1, ...], col1_index: [<PageRef>, ...]
            colDiskPartial (lst): [ColumnIndex_1, ColumnIndex_1, ...], col1_index: [<PageRef>, ...]
            colDiskFull (lst): [ColumnIndex_1, ColumnIndex_1, ...], col1_index: [<PageRef>, ...]
        """

        self.pageDirectoryCapacity = 10000000
        self.pageDirectory = {} 
        self.path = ""

        # col-mem-partial & col-mem-full make up the complete buffer(memory) pool of Page objects
        self.colMemPartial = []
        self.colMemFull = []

        # col-disk-partial & col-disk-full make up the pageIds of all the pages stored on disk 
        self.colDiskPartial = []
        self.colDiskFull = []
        for i in range(num_columns):
            self.colMemPartial.append([])
            self.colMemFull.append([])
            self.colDiskPartial.append([])
            self.colDiskFull.append([])

        self.numPages = 0
        self.capacity = maxPages

        self.pageCount = 0
        self.dirtyPageList = [None] * self.pageDirectoryCapacity
        self.dirtyPageListTail = 0

    def hasCapacity(self):
        return self.numPages < self.capacity   

    def getMemPages(self):
        """
        Description: This function creates a new page and adds it to the specified column index

        Inputs: 
            N/A
        Outputs:
            Returns a list of tuples. Format: [(ref, LFU, columnIdx, indx), ...]
        
        """
        result = []
        for i, column in enumerate(self.colMemPartial):
            for j, page in enumerate(column):
                result.append((page, self.colMemPartial, page.calculateLFU(), i, j))

        for i, column in enumerate(self.colMemFull):
            for j, page in enumerate(column):
                result.append((page, self.colMemFull, page.calculateLFU(), i, j))

        return result   

    def _createPage(self, columnIdx):
        """
        Description: This function creates a new page and adds it to the specified column index

        Inputs: 
            columnIdx (int): An integer index of the column.
        Outputs:
            Returns the page ID of the created page
        """
        #print(f"        [BufferPool.createPage] createPage called making a new page for column: {columnIdx}!") 
        PID = "P-" + str(self.pageCount)
        #print(f"        [BufferPool.createPage] PID: {PID} - Creating new page object!")
        page = Page(PID, self.path)
        #print(f"        [BufferPool.createPage] page created! page.pageID: {page.pageID}")
        #print(f"        [BufferPool.createPage] Saving initial version of the page!")
        page.save("-partial") #initial save to create the files
        #print(f"        [BufferPool.createPage] Page Saved! Adding new entry into the pageDirectory!")
        entry = (0, 1, columnIdx, len(self.colMemPartial[columnIdx]))
        #print(f"        [BufferPool.createPage] New entry: {entry}")
        self.pageDirectory[PID] = entry
        #print(f"        [BufferPool.createPage] Adding new page to memory!")
        #print(f"        [BufferPool.createPage] Memory buffer pre-append: {self.colMemPartial}")
        #print(f"        [BufferPool.createPage] Column pages: {self.colMemPartial[columnIdx]}")
        self.colMemPartial[columnIdx].append(page)
        #print(f"        [BufferPool.createPage] Updated memory: {self.colMemPartial}")
        self.pageCount += 1
        #print(f"        [BufferPool.createPage] pageCount: {self.pageCount}")
        self.numPages += 1
        #print(f"        [BufferPool.createPage] numPages: {self.numPages}")
        #print(f"        [BufferPool.createPage] Complete! returning page reference: {page.pageID}")
        return page
    
    def getPage(self, PID="Default", columnIdx=-1):
        #print(f"    [BufferPool.getPage] getPage called with PID: {PID} - columnIdx: {columnIdx}")
        ret = None
        #print(f"    [BufferPool.getPage] Checking if the pageID exists in memory...")
        if(PID in self.pageDirectory): #if page exists
            #print(f"    [BufferPool.getPage] The page with ID: {PID} exists! Fetching the page directory entry...")
            index = self.pageDirectory[PID]
            #print(f"    [BufferPool.getPage] Complete! index: {index}")
            #print(f"    [BufferPool.getPage] Checking if the page is in memory...")
            if(index[1]): #if page in memory then return the page reference
                #print(f"    [BufferPool.getPage] Page {PID} in memory. index[1]: {index[1]}")
                #print(f"    [BufferPool.getPage] Checking if page is full...")
                if(index[0]): #returning a full page
                    #print(f"    [BufferPool.getPage] Page is full! index[0]: {index[0]}")
                    #print(f"    [BufferPool.getPage] Fetching page from colMemFull at column: {index[2]} - {index[3]}")
                    #print(f"    [BufferPool.getPage] {[page.pageID for page in self.colMemFull[index[2]]]}")
                    for page in self.colMemFull[index[2]]:
                        if page.pageID == PID: 
                            ret = page 
                    #ret = self.colMemFull[index[2]][index[3]]
                    #print(f"    [BufferPool.getPage] Got page {ret.pageID}!")
                else:
                    #print(f"    [BufferPool.getPage] Page is partial! index[0]: {index[0]}")
                    #print(f"    [BufferPool.getPage] Fetching partial page from memory! column: {index[2]} - pageIdx:{index[3]}")
                    for page in self.colMemPartial[index[2]]:
                        if page.pageID == PID: 
                            ret = page 
                    #print(f"    [BufferPool.getPage] Got page: {ret.pageID}")
            else: #else the page is in disk
                #print(f"    [BufferPool.getPage] The page is not in memory! Checking the disk...")
                #print(f"    [BufferPool.getPage] Checking if we have space to load a page: {self.numPages < self.capacity}")
                if(self.numPages < self.capacity): #if we have space then load the page
                    #print(f"    [BufferPool.getPage] We have space in the bufferpool. Loading page: {PID}")
                    self.loadPage(PID)
                    #print(f"    [BufferPool.getPage] Page loaded into memory!")
                    self.numPages += 1
                    #print(f"    [BufferPool.getPage] numPages: {self.numPages}. Getting new index object...")
                    index = self.pageDirectory[PID] #new index post load
                    #print(f"    [BufferPool.getPage] index: {index}")
                    if(index[0]): #returning a full page
                        #print(f"    [BufferPool.getPage] The page is full after load! Returning a reference to the full page!")
                        for page in self.colMemFull[index[2]]:
                            if page.pageID == PID: 
                                ret = page 
                        #print(f"    [BufferPool.getPage] returning page: {ret.pageID}")
                    else:
                        #print(f"    [BufferPool.getPage] Returning a partial page after load!")
                        for page in self.colMemPartial[index[2]]:
                            if page.pageID == PID: 
                                ret = page 
                        #print(f"    [BufferPool.getPage] partial: {ret.pageID}")
                else: #We don't have space so evict the page then load the new page
                    #print(f"    [BufferPool.getPage] No space for new pages. Evicting page!")
                    self.evict()
                    #print(f"    [BufferPool.getPage] Pages evicted! We now have more space. loading new page: {PID}")
                    self.loadPage(PID)
                    #print(f"    [BufferPool.getPage] page Loaded!")
                    self.numPages += 1
                    #print(f"    [BufferPool.getPage] numPages: {self.numPages}")
                    index = self.pageDirectory[PID] #new index post load
                    #print(f"    [BufferPool.getPage] New index after load: {index}")
                    if(index[0]): #returning a full page
                        #print(f"    [BufferPool.getPage] This is a full page!")
                        for page in self.colMemFull[index[2]]:
                            if page.pageID == PID: 
                                ret = page 
                        #print(f"    [BufferPool.getPage] returning full page: {ret.pageID}")
                    else:
                        #print(f"    [BufferPool.getPage] Returning partial page...")
                        for page in self.colMemFull[index[2]]:
                            if page.pageID == PID: 
                                ret = page 
                        #print(f"    [BufferPool.getPage] Returning page: {ret.pageID}")
        else: #page doesn't exist so create a new page and return it's reference.
            #print(f"    [BufferPool.getPage] no pages found for id: {PID}")
            #print(f"    [BufferPool.getPage] Creating new page in column: {columnIdx}")
            ret = self._createPage(columnIdx)
            #print(f"    [BufferPool.getPage] Created page: {ret.pageID}!")
        #print(f"    [BufferPool.getPage] Finished. Returning page...")
        return ret
    
    def deletePage(self, PID):
        """
        Description: This function deletes a page and all of it's data including the data on disk.

        Inputs: 
            PID (str): The Page ID of the page we want to delete.
        Outputs:
            True if successful - else False
        """
        ret = True
        if(PID in self.pageDirectory):
            index = self.pageDirectory[PID] #index: (isFull, inMem, columnIdx, pageIdx)
            if(index[1]): #if the page is in memory
                if(index[0]): #if page is full
                    self.colMemFull[index[2]][index[3]].delete() #remove any saved files
                    self.colMemFull[index[2]].pop(index[3]) #remove from memory
                    del self.pageDirectory[PID] #remove from page directory
                else: #page is partially full
                    self.colMemPartial[index[2]][index[3]].delete() #remove any saved files
                    self.colMemPartial[index[2]].pop(index[3]) #remove from memory
                    del self.pageDirectory[PID] #remove from page directory
            else: #The page is on disk so just delete the files
                if(index[0]): #if page is full
                    self.colDiskFull[index[2]].pop(index[3]) #remove from memory
                    remove(f"{self.path}{PID}-full.offsets")
                    remove(f"{self.path}{PID}-full.bin")
                    del self.pageDirectory[PID] #remove from page directory
                else: #page is partially full
                    self.colDiskFull[index[2]].pop(index[3]) #remove from memory
                    remove(f"{self.path}{PID}-partial.offsets")
                    remove(f"{self.path}{PID}-partial.bin")
        else:
            ret = False
        return ret

    def savePage(self, PID):
        """
        Description: This method writes the page to disk and removes it from the active bufferpool.
        Inputs:
            pageID(str): The ID of the page we want to write to disk.
        """
        ret = True
        if(PID in self.pageDirectory): #if the page exists
            index = self.pageDirectory[PID] #index: (isFull, inMem, columnIdx, pageIdx)
            if(index[1]): #the page is in memory, then save it else it's in disk, no save necessary.
                if(index[0]): #if the page is full
                    page = self.colMemFull[index[2]].pop(index[3]) #pop full page from memory
                    page.save("-full") #Save the page
                    index = (1, 0, index[2], len(self.colDiskFull))
                    self.colDiskFull[index[2]].append(page.pageID)
                    self.pageDirectory[PID] = index
                else:
                    page = self.colMemPartial[index[2]].pop(index[3])
                    page.save("-partial")
                    index = (0, 0, index[2], len(self.colDiskFull))
                    self.colDiskPartial[index[2]].append(page.pageID)
                    self.pageDirectory[PID] = index
            else:
                ret = False
        return ret
    
    def loadPage(self, PID):
        ret = True
        if(PID in self.pageDirectory): #if the page exists
            index = self.pageDirectory[PID] #index: (isFull, inMem, columnIdx, pageIdx)
            if(not index[1]): #the page is in disk then load it else it's in memory, no load necessary.
                if(index[0]): #if the page is full
                    self.colDiskFull[index[2]].pop(index[3]) #pop full page PID from disk
                    page = Page(PID, self.path)
                    page.load('-full')
                    index = (index[0], 1, index[2], len(self.colMemFull))
                    #print(f"INDEX WHEN RETRIEVING FROM DISK FULL {index}")
                    self.colMemFull[index[2]].append(page)
                    self.pageDirectory[PID] = index
                else:
                    self.colDiskPartial[index[2]].pop(index[3]) #pop partial page PID from disk
                    page = Page(PID, self.path)
                    page.load('-partial')
                    index = (index[0], 1, index[2], len(self.colMemPartial))
                    #print(f"INDEX WHEN RETRIEVING FROM PARTIAL FULL {index}")
                    self.colMemPartial[index[2]].append(page)
                    self.pageDirectory[PID] = index
            else:
                ret = False
        return ret
    
    def evict(self):
        """
        Evicts 60% of the pages in memory using the LFU (Least Frequently Used) strategy.
        Moves evicted pages to disk and updates metadata accordingly.
        """
        # Collect all pages and their LFU values

        pages_in_mem = self.getMemPages()
        sorted_pages = sorted(pages_in_mem, key = lambda x: x[2]) # (page, col_mem_full/ col_mem_partial, page.calculateLFU(), columnIndex, j)

        
        num_to_evict = int(0.4 * len(pages_in_mem))

        for i in range(1, num_to_evict + 1):
            tuple_to_evict = sorted_pages[-i]
            page = tuple_to_evict[0]

            if page.isDirty:
                page.save()

                
            array_to_remove_from = tuple_to_evict[1]
            col_index = tuple_to_evict[3]
            index_in_col = tuple_to_evict[4]

            
            array_to_remove_from[col_index].pop(index_in_col)