import logging
from lstore.page import Page
from lstore.setupLogging import *
from os import remove

class BufferPool:

    def __init__(self, numColumns, path, maxPages=50):
        """
        Description: The BufferPool Memory Manager for a given table
        Inputs: 
            numColumns (int): The number of columns for this table.
            path (str): The path variable to the location on disk for storing files.
            maxPages (int): The maximum number of pages that can be stored in memory for each column.
        Outputs:
            N\A 
        
        Internal Objects:
            pageDirectory (dict): Maps Page IDs to metadata about that page. 
                                  Format: {<pid> : (isFull, inMem, columnIdx, pageIdx)}
            path (str) : The path for the bufferpool. Composed of: dbDir/tableDir/
            partialMemPages (lst): [ColIdx_1, ColIdx_2, ..., colIndex_|numCol|] -> colIdx_i: [<PageRef_1>, ...]
            fullMemPages (lst): [ColIdx_1, ColIdx_2, ..., colIndex_|numCol|] -> colIdx_i: [<PageRef_1>, ...]
            colDiskPartial (lst): [ColIdx_1, ColIdx_2, ..., colIndex_|numCol|] -> colIdx_i: ["<PID>", ...]
            colDiskFull (lst): [ColIdx_1, ColIdx_2, ..., colIndex_|numCol|] -> colIdx_i: ["<PID>", ...]
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.log = setupLogger(False, "DEBUG", self.log, 8)

        self.log.debug(f"BufferPool constructor called! Creating new bufferPool object with params - numColumns: {numColumns} - path: {path} - maxPages: {maxPages}")
        self.pageDirectory = {} 
        self.path = path
        self.activePages = 0 #current number of active pages
        self.pageCount = 0 #Total number of pages
        self.capacity = maxPages

        # partialMemPages & fullMemPages make up the complete buffer(memory) pool of Page objects
        self.partialMemPages = [[] for x in range(numColumns)]
        self.fullMemPages = [[] for x in range(numColumns)]

        # col-disk-partial & col-disk-full make up the pageIds of all the pages stored on disk 
        self.partialDiskPages = [[] for x in range(numColumns)]
        self.fullDiskPages = [[] for x in range(numColumns)]
        self.log.debug(f"BufferPool created! Current bufferpool:\nself.partialMemPages: {self.partialMemPages}\nself.fullMemPages: {self.fullMemPages}\nself.partialDiskPages: {self.partialDiskPages}\nself.fullDiskPages:{self.fullDiskPages}")

    def hasCapacity(self):
        self.log.debug(f"Checking if the bufferpool has capacity for new pages: Number of active pages: {self.activePages} - capacity: {self.capacity} - conditional: {self.activePages < self.capacity}")
        return self.activePages < self.capacity   

    def getMemPages(self):
        """
        Description: This function returns a 1-D list of all pages in memory

        Inputs: 
            N/A
        Outputs:
            Returns a list of tuples. Format: [(<page ref>, LFU, columnIdx, pageIdx), ...]
        """
        result = []
        self.log.debug(f"getMemPages called! Creating list of all memory pages in the bufferpool...")
        self.log.debug(f"Fetching partially filled pages and adding to list...")
        for i, column in enumerate(self.partialMemPages):
            self.log.debug(f"Fetching all mempages for column: {i}...")
            for j, page in enumerate(column):
                lfu = page.calculateLFU()
                self.log.debug(f"Appending page {page.pageID} to the list: ({page.pageID}, {lfu}, {i}, {j})")
                result.append((page, lfu, i, j))
        self.log.debug(f"Complete! Fetching full memory pages and adding to list...")
        for i, column in enumerate(self.fullMemPages):
            self.log.debug(f"Fetching all mempages for column: {i}...")
            for j, page in enumerate(column):
                lfu = page.calculateLFU()
                self.log.debug(f"Appending page {page.pageID} to the list: ({page.pageID}, {lfu}, {i}, {j})")
                result.append((page, lfu, i, j))
        self.log.debug(f"Complete! Returning list of pages:\n{result}")
        return result   

    def _createPage(self, columnIdx):
        """
        Description: This function creates a new page and adds it to the specified column

        Inputs: 
            columnIdx (int): An integer entry of the column.
        Outputs:
            Returns the page ID of the created page
        Notes:
            Page dir Format: {<pid> : (isFull, inMem, columnIdx, pageIdx)}
        """
        self.log.debug(f"createPage called! Building a new page and adding it to column: {columnIdx}...") 
        PID = "P-" + str(self.pageCount)
        self.log.debug(f"New PID: {PID}. Creating new page object!")
        page = Page(PID, self.path)
        self.log.debug(f"New Page object created! <page>.pageID: {page.pageID}")
        self.log.debug(f"Saving initial version of the page....")
        page.save("-partial") #initial save to create the files
        self.log.debug(f"Page Saved! Creating new entry into the pageDirectory...")
        entry = (0, 1, columnIdx, len(self.partialMemPages[columnIdx]))
        self.log.debug(f"New entry: {PID} -> {entry}")
        self.pageDirectory[PID] = entry
        self.log.debug(f"Adding new page to memory! Entire Memory buffer pre-append: {self.partialMemPages}")
        self.log.debug(f"Column pages pre-append: {self.partialMemPages[columnIdx]}")
        self.partialMemPages[columnIdx].append(page)
        self.log.debug(f"Adding new page to memory! Entire Memory buffer post-append: {self.partialMemPages}")
        self.log.debug(f"Column pages post-append: {self.partialMemPages[columnIdx]}")
        self.pageCount += 1
        self.log.debug(f"pageCount: {self.pageCount}")
        self.activePages += 1
        self.log.debug(f"numPages: {self.activePages}")
        self.log.debug(f"Complete! Returning page reference for new page: {page.pageID}")
        return page

    def getPage(self, PID="default", columnIdx=-1):
        """
        Description: This method returns a page reference when given a page ID.
                     If no page ID is given But a columnIdx is given then this method creates a new page, appends it to that column and returns the page reference.
                     Automatically handles eviction if the page requested is from disk and the buffer is full.
        Inputs: 
            PID (str): The string page-ID of the page you want to fetch.
            columnIdx (int): The entry of a column you want to add a new page to.
        Outputs: 
            <PageRef> : A python reference to the requested page.
        Notes:
            Page dir Format: {<pid> : (isFull, inMem, columnIdx, pageIdx)}
        """
        self.log.debug(f"getPage called! Params -> PID: {PID} - columnIdx: {columnIdx}")
        if(PID == "default" and columnIdx == -1):
            self.log.error(f"ERROR: getpage called without a pageID or column entry!")
            return None
        ret = None
        self.log.debug(f"Checking getPage() mode...")
        if(PID in self.pageDirectory): #if page exists
            self.log.debug(f"The page with ID: {PID} exists! Fetching the page directory entry...")
            entry = self.pageDirectory[PID]
            self.log.debug(f"Complete! entry: {entry}")
            self.log.debug(f"Entry format: (isFull, inMem, columnIdx, pageIdx)")
            self.log.debug(f"Checking if the page is in memory...")
            if(entry[1]): #if page in memory then return the page reference
                self.log.debug(f"Page {PID} in memory! entry[1]: {entry[1]}")
                self.log.debug(f"Checking if the page is full...")
                if(entry[0]): #returning a full page
                    self.log.debug(f"The page {PID} is full! entry[0]: {entry[0]}")
                    self.log.debug(f"Fetching page from fullMemPages at column: {entry[2]} entry: {entry[3]}")
                    self.log.debug(f"self.fullMemPages[{entry[2]}]: {[page.pageID for page in self.fullMemPages[entry[2]]]}")
                    ret = self.fullMemPages[entry[2]][entry[3]]
                    self.log.debug(f"Got page {ret.pageID}!")
                else:
                    self.log.debug(f"The page {PID} is partially filled! entry[0]: {entry[0]}")
                    self.log.debug(f"Fetching page from partialMemPages at column: {entry[2]} entry: {entry[3]}")
                    self.log.debug(f"self.partialMemPages[{entry[2]}]: {[page.pageID for page in self.partialMemPages[entry[2]]]}")
                    ret = self.partialMemPages[entry[2]][entry[3]]
                    self.log.debug(f"Got page {ret.pageID}!")
            else: #else the page is in disk
                self.log.debug(f"The page is not in memory! entry[1]: {entry[1]}")
                self.log.debug(f"Checking if we have space to load a page from disk...")
                if(not self.hasCapacity()): #if we don't have space then evict, nmw load new page once we have space
                    self.log.debug(f"No space for new pages. Evicting the bottom 40% of the least frequently used pages!")
                    self.evict()
                    self.log.debug(f"Pages evicted! We now have more space...")
                else:
                    self.log.debug(f"We have the space to load new pages from disk!")
                self.log.debug(f"Loading page from disk: {PID}")
                self.loadPage(PID)
                self.log.debug(f"Page loaded into memory!")
                self.log.debug(f"Number of Pages in memory: {self.activePages}. Getting page entry for page {PID} ...")
                entry = self.pageDirectory[PID] #new entry is created during loadPage so refetch it here
                self.log.debug(f"New entry: {entry}")
                self.log.debug(f"Entry format: (isFull, inMem, columnIdx, pageIdx)")
                if(entry[0]): #returning a full page
                    self.log.debug(f"self.fullMemPages[{entry[2]}]: {[page.pageID for page in self.fullMemPages[entry[2]]]}")
                    ret = self.fullMemPages[entry[2]][entry[3]]
                    self.log.debug(f"Got page: {ret.pageID}!")
                else:
                    self.log.debug(f"self.partialMemPages[{entry[2]}]: {[page.pageID for page in self.partialMemPages[entry[2]]]}")
                    ret = self.partialMemPages[entry[2]][entry[3]]
                    self.log.debug(f"Got page: {ret.pageID}!")
        else: #page doesn't exist so create a new page and return it's reference.
            self.log.debug(f"Get page called in create mode! Creating new page and appending to column: {columnIdx}")
            ret = self._createPage(columnIdx)
            self.log.debug(f"Created page: {ret.pageID}!")
        self.log.debug(f"getPage Finished! Returning page with ID: {ret.pageID}")
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
        self.log.debug(f"deletePage called! Deleting the page with PID: {PID}")
        if(PID in self.pageDirectory): #if the page exists
            self.log.debug(f"Page located in directory! Fetching directory entry...")
            entry = self.pageDirectory[PID] #entry: (isFull, inMem, columnIdx, pageIdx)
            self.log.debug(f"Complete! entry: {entry}")
            self.log.debug(f"Entry format: (isFull, inMem, columnIdx, pageIdx)")
            if(entry[1]): #if the page is in memory
                self.log.debug(f"Page is in memory! entry[1]: {entry[1]}")
                if(entry[0]): #if page is full
                    self.log.debug(f"Page is full! Popping page at location: column: {entry[2]} - entry: {entry[1]}")
                    page = self.fullMemPages[entry[2]].pop(entry[3])
                    self.log.debug(f"Deleting page: {page.pageID}")
                    page.delete() #remove any saved files
                    self.log.debug(f"Page deleted! Removing entry from page directory...")
                    del self.pageDirectory[PID] #remove from page directory
                    self.log.debug(f"Page removed from directory and memory.")
                else: #page is partially full
                    self.log.debug(f"Page is full! Popping page at location: column: {entry[2]} - entry: {entry[1]}")
                    page = self.partialMemPages[entry[2]].pop(entry[3])
                    self.log.debug(f"Deleting page: {page.pageID}")
                    page.delete() #remove any saved files
                    self.log.debug(f"Page deleted! Removing entry from page directory...")
                    del self.pageDirectory[PID] #remove from page directory
                    self.log.debug(f"Page removed from directory and memory.")
            else: #The page is on disk so just delete the files
                self.log.debug(f"Deleting page files on disk...")
                if(entry[0]): #if page is full
                    self.log.debug(f"Deleting full page from disk...")
                    page = self.fullDiskPages[entry[2]].pop(entry[3]) #remove from memory
                    self.log.debug(f"Deleting page with ID: {page.pageID}! Removing offset: {self.path}{PID}-full.offsets")
                    remove(f"{self.path}{PID}-full.offsets")
                    self.log.debug(f"Complete! Removing data file: {self.path}{PID}-full.bin")
                    remove(f"{self.path}{PID}-full.bin")
                    self.log.debug(f"Complete! Removing entry from page directory...")
                    del self.pageDirectory[PID] #remove from page directory
                    self.log.debug(f"Entry removed!")
                else: #page is partially full
                    self.log.debug(f"Deleting partial page from disk...")
                    page = self.partialDiskPages[entry[2]].pop(entry[3]) #remove from memory
                    self.log.debug(f"Deleting page with ID: {page.pageID}! Removing offset: {self.path}{PID}-partial.offsets")
                    remove(f"{self.path}{PID}-partial.offsets")
                    self.log.debug(f"Complete! Removing data file: {self.path}{PID}-partial.bin")
                    remove(f"{self.path}{PID}-partial.bin")
                    self.log.debug(f"Complete! Removing entry from page directory...")
                    del self.pageDirectory[PID] #remove from page directory
                    self.log.debug(f"Entry removed!")
        else:
            self.log.debug(f"Page with ID {PID} not found! Returning False...")
            ret = False
        self.log.debug(f"Complete! Return Value: {ret}")
        return ret

    def savePage(self, PID):
        """
        Description: This method writes the page to disk and removes it from the active bufferpool.
        Inputs:
            pageID(str): The ID of the page we want to write to disk.
        Notes:
            Page dir Format: {<pid> : (isFull, inMem, columnIdx, pageIdx)}
        """
        ret = True
        self.log.debug(f"savePage called! Writing the page with ID {PID} to disk...")
        if(PID in self.pageDirectory): #if the page exists
            self.log.debug(f"Page found! Fetching page directory entry...")
            entry = self.pageDirectory[PID] #entry: (isFull, inMem, columnIdx, pageIdx)
            self.log.debug(f"Entry found for page {PID}: {entry}")
            self.log.debug(f"Page dir Format: PID -> (isFull, inMem, columnIdx, pageIdx)")
            self.log.debug(f"Checking if the page is in memory...")
            if(entry[1]): #the page is in memory, then save it else it's in disk, no save necessary.
                self.log.debug(f"Page is in memory! Checking if it is full...")
                if(entry[0]): #if the page is full
                    self.log.debug(f"Full page detected! Saving to disk...")
                    self.log.debug(f"fullMemPages pre-pop: {[page.pageID for page in self.fullMemPages[entry[2]]]}")
                    page = self.fullMemPages[entry[2]].pop(entry[3]) #pop full page from memory
                    self.log.debug(f"fullMemPages post-pop: {[page.pageID for page in self.fullMemPages[entry[2]]]}")
                    self.log.debug(f"Popped page: {page.pageID}! Saving to disk if the page is dirty...")
                    if(page.isDirty):
                        self.log.debug(f"Dirty page detected! Saving to disk")
                        page.save("-full") #Save the page
                    self.log.debug(f"Page saved to disk! Creating new entry for page directory...")
                    entry = (1, 0, entry[2], len(self.colDiskFull))
                    self.log.debug(f"New entry: {entry}! Appending to fullDiskPages...")
                    self.log.debug(f"fullDiskPages pre-append: {[page.pageID for page in self.fullDiskPages[entry[2]]]}")
                    self.fullDiskPages[entry[2]].append(page.pageID)
                    self.log.debug(f"fullDiskPages post-append: {[page.pageID for page in self.fullDiskPages[entry[2]]]}")
                    self.log.debug(f"Updating page directory - old: {self.pageDirectory[PID]}")
                    self.pageDirectory[PID] = entry
                    self.log.debug(f"Updating page directory - new: {self.pageDirectory[PID]}")
                else:
                    self.log.debug(f"Partial page detected! Saving to disk...")
                    self.log.debug(f"partialMemPages pre-pop: {[page.pageID for page in self.partialMemPages[entry[2]]]}")
                    page = self.partialMemPages[entry[2]].pop(entry[3]) #pop partial page from memory
                    self.log.debug(f"fullMemPages post-pop: {[page.pageID for page in self.partialMemPages[entry[2]]]}")
                    self.log.debug(f"Popped page: {page.pageID}! Saving to disk...")
                    if(page.isDirty):
                        self.log.debug(f"Dirty page detected! Saving to disk")
                        page.save("-partial") #Save the page
                    self.log.debug(f"Page saved to disk! Creating new entry for page directory...")
                    entry = (1, 0, entry[2], len(self.partialDiskPages))
                    self.log.debug(f"New entry: {entry}! Appending to partialDiskPages...")
                    self.log.debug(f"partialDiskPages pre-append: {[page.pageID for page in self.partialDiskPages[entry[2]]]}")
                    self.partialDiskPages[entry[2]].append(page.pageID)
                    self.log.debug(f"partialDiskPages post-append: {[page.pageID for page in self.partialDiskPages[entry[2]]]}")
                    self.log.debug(f"Updating page directory - old: {self.pageDirectory[PID]}")
                    self.pageDirectory[PID] = entry
                    self.log.debug(f"Updating page directory - new: {self.pageDirectory[PID]}")
            else:
                self.log.debug(f"ERROR! Page is already on disk! No point saving. Exiting...")
                ret = False
        self.log.debug(f"Complete! Return Value: {ret}")
        return ret
    
    def loadPage(self, PID):
        ret = True
        self.log.debug(f"loadPage called! Loading page with PID: {PID} from disk...")
        self.log.debug(f"checking if page in directory...")
        if(PID in self.pageDirectory): #if the page exists
            self.log.debug(f"Page found! Fetching page directory entry...")
            entry = self.pageDirectory[PID] #entry: (isFull, inMem, columnIdx, pageIdx)
            self.log.debug(f"Entry found for page {PID}: {entry}")
            self.log.debug(f"Entry Format: PID -> (isFull, inMem, columnIdx, pageIdx)")
            if(not entry[1]): #the page is in disk then load it else it's in memory, no load necessary.
                self.log.debug(f"Page is located on disk! Loading into memory...")
                if(entry[0]): #if the page is full
                    self.log.debug(f"Full page detected! popping from disk full...")
                    self.log.debug(f"fullDiskPages pre-pop: {[page for page in self.fullDiskPages[entry[2]]]}")
                    PID = self.fullDiskPages[entry[2]].pop(entry[3]) #pop full page from disk
                    self.log.debug(f"fullDiskPages post-pop: {[page for page in self.fullDiskPages[entry[2]]]}")
                    self.log.debug(f"Creating live page for: {PID}")
                    page = Page(PID, self.path)
                    self.log.debug(f"Page object with ID {page.pageID} created! loading data from disk...")
                    page.load('-full')
                    self.log.debug(f"Data loaded! Creating new entry for page directory...")
                    entry = (entry[0], 1, entry[2], len(self.fullMemPages))
                    self.log.debug(f"New entry: {entry}! Appending to fullMemPages...")
                    self.log.debug(f"fullMemPages pre-append: {[page.pageID for page in self.fullMemPages]}")
                    self.fullMemPages[entry[2]].append(page)
                    self.log.debug(f"fullMemPages post-append: {[page.pageID for page in self.fullMemPages]}")
                    self.log.debug(f"Updating page directory - old: {self.pageDirectory[PID]}")
                    self.pageDirectory[PID] = entry
                    self.log.debug(f"Updating page directory - new: {self.pageDirectory[PID]}")
                else:
                    self.log.debug(f"Partial page detected! popping from disk full...")
                    self.log.debug(f"partialDiskPages pre-pop: {[page for page in self.partialDiskPages[entry[2]]]}")
                    PID = self.partialDiskPages[entry[2]].pop(entry[3]) #pop full page from disk
                    self.log.debug(f"partialDiskPages post-pop: {[page for page in self.partialDiskPages[entry[2]]]}")
                    self.log.debug(f"Creating live page for: {PID}")
                    page = Page(PID, self.path)
                    self.log.debug(f"Page object with ID {page.pageID} created! loading data from disk...")
                    page.load('-partial')
                    self.log.debug(f"Data loaded! Creating new entry for page directory...")
                    entry = (entry[0], 1, entry[2], len(self.partialMemPages))
                    self.log.debug(f"New entry: {entry}! Appending to fullMemPages...")
                    self.log.debug(f"partialMemPages pre-append: {[page.pageID for page in self.partialMemPages]}")
                    self.partialMemPages[entry[2]].append(page)
                    self.log.debug(f"partialMemPages post-append: {[page.pageID for page in self.partialMemPages]}")
                    self.log.debug(f"Updating page directory - old: {self.pageDirectory[PID]}")
                    self.pageDirectory[PID] = entry
                    self.log.debug(f"Updating page directory - new: {self.pageDirectory[PID]}")
            else:
                self.log.debug(f"Page is already in memory. No load necessary...")
                ret = False
        self.log.debug(f"Complete! Return value: {ret}")
        return ret
    
    def evict(self):
        """
        Evicts 60% of the pages in memory using the LFU (Least Frequently Used) strategy.
        Moves evicted pages to disk and updates metadata accordingly.
        """
        # Collect all pages and their LFU values
        self.log.debug(f"evict called! Running eviction algorithm...")
        self.log.debug(f"Building a list of all in-memory pages...")
        memPages = self.getMemPages()
        self.log.debug(f"Complete! memPages: {memPages}")
        sorted_pages = sorted(memPages, key = lambda x: x[1]) # (page, LFU, columnIdx, pageIdx)
        self.log.debug(f"Sorted memPages: {sorted_pages}")
        num_to_evict = int(0.4 * len(sorted_pages))
        self.log.debug(f"Evicting: {num_to_evict} lowest rated pages...")
        for i in range(1, num_to_evict + 1):
            tuple_to_evict = sorted_pages[-i]
            page = tuple_to_evict[0]
            self.log.debug(f"Evicting page: {page.pageID}")
            self.savePage(page.pageID)
        self.log.debug(f"evict complete!")