"""
Documentation for the table(database) class.
Author: Jack O'Donnell Jodonnel@uoregon.edu 
Description:
    This file contains the implementation for the table. 

"""
from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns, bufferpool, indexCols):
        self.rid = rid
        self.key = key

        self.bufferPool = bufferpool
        self.indexCols = indexCols

        self.columns = [0] * len(columns)
        #Change columns to hold list of page ids
        # self.columns = columns
        self.pageCols = columns

        self.indirection = None

    def copy(self):
        new_cols = [column for column in self.pageCols]
        return Record(self.rid, self.key, new_cols, self.bufferPool, self.indexCols)

    def getRecord(self):
        cols = [0] * len(self.pageCols)
        for i, pageID in enumerate(self.pageCols):
            page = self.bufferPool.getPageById(pageID)
            cols[i] = int(page.read(self.indexCols[i])) #int.from_bytes(page.read(self.indexCols[i]), byteorder='big', signed=False)
        self.columns = cols 
        return self
    
    def getColumnValue(self, index):
        page = self.bufferPool.getPageById(self.pageCols[index])
        return int(page.read(self.indexCols[index]))        


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferPool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.bufferPool = bufferPool

        # self.base_pages = [[Page()] for i in range(num_columns)] #List of List [[Base Pages for column 1], [Base Pages for column 2], [Base Pages for column 3]]
        # self.tail_pages = [[Page()] for i in range(num_columns)] #List of List [Tail Pages for column 1], [Tail Pages for column 2], [Tail Pages for column 3]]
        

        # HJV changes
        self.base_pages = [[bufferPool.createNewPageAndGetIdForColumn(i)] for i in range(num_columns)] # List of List of page ids [[Base pageID for column 1], [Base pageID for column 2], [Base pageID for column 3]]
        self.tail_pages = [[bufferPool.createNewPageAndGetIdForColumn(i)] for i in range(num_columns)]# List of List of page ids [[Tail pageID for column 1], [Tail pageID for column 2], [Tail pageID for column 3]]

        
        self.bp_directory = dict() # Maps RID -> Base Record 
        self.tp_directory = dict() # Maps BaseRecord.rid -> [TailRecord, TailRecord, Tailrecord]
        
        
        self.bp_index = [0] * num_columns #List of stored index for base pages. [0, 1, 2] access self.base_pages[i][self.bp_index[i]] 
        self.tp_index = [0] * num_columns #List of stored index for tail pages. [0, 1, 2] access self.tail_pages[i][self.bp_index[i]] 
        self.key_rid = dict() #Maps key->rid can use to find all rids with that key 

        pass

    def insert(self, *columns):
        """
        RID is stored as a list of tuples. each tuple represents a base page location and offset. 
        Each tuple in the list corresponds to a column/page for that column. 
        For example, RID = [(0,2), (3,2), (1,1)] -> The second column value is stored in self.base_pages[1][3] and is the 2nd element in that page 
        self.bp_index is used to track the index of the current base page as it fills 

        record_location = ()
        for i, value in enumerate(columns[0]):                  #LOOP THROUGH COLUMNS                            
            if col-mem-partial[i]:                              #IF COLUMN HAS PARTIAL PAGES IN MEMORY                          
                page = col-mem-partial[i][0]                    #RETRIEVE THE PAGE 
                page.write(value)                               #WRITE VALUE TO THE PAGE 
                record_location[i] = (page.pageID, offset)      #SAVE THE LOCATION FOR THAT COLUMN 
                if not page.hasCapacity():                      #IF THE PAGE IS FULL AFTER INSERT 
                    col-mem-full[i].append(page)                #MOVE IT TO FULL PAGES IN MEMORY 
                    col-mem-partial[i].remove(page)             #REMOVE IT FROM THE PARTIAL PAGES IN MEMORY 

            else:                                                   #IF THERE ARE NO PARTIAL PAGES IN MEMORY 
                if not bufferpool.hasCapacityForColumn(i):          #IF BUFFERPOOL IS FULL (OF FULL PAGES)
                    bufferpool.evict()                              #EVICT A PAGE BASED ON POLICY NOW THERE IS AN EMPTY SPACE IN MEMORY  
                    if col-disk-partial[i]:                         #IF THERE ARE PARTIAL PAGES ON THE DISK 
                        page = col-disk-partial[i][0]               #RETRIEVE THE PAGE FROM DISK TO MEMORY 
                        page.write(value)                           #WRITE VALUE TO THE PAGE 
                        record_location[i] = (page.pageID, offset)  #SAVE THE LOCATION FOR THIS COLUMN 
                        if not page.hasCapacity():                  #IF THE PAGE IS NOW FULL 
                            col-mem-full[i].append(page)            #ADD IT TO FULL PAGES IN MEMORY 
                        else:                                       #IF PAGE IS PARTIAL STILL 
                            col-mem-partial[i].append(page)         #ADD IT TO PARTIAL PAGES IN MEMORY 
                        
                    else:                                                   #IF THERE ARE NO PARTIAL PAGES ON DISK 
                        pid = createNewPageAndGetId()                       #CREATE A NEW PAGE AND GET PAGE ID 
                        bufferpool.pageDir[pid].write(value)                #WRITE VALUE TO THE PAGE 
                        col-mem-partial[i].append(bufferpool.pageDir[pid])  #ADD THE PAGE TO PARTIAL PAGES IN MEMORY 
                        record_location[i] = (pid, offset)                  #SAVE THE LOCATION FOR THAT COLUMN 

                else:                                                       #IF THE BUFFERPOOL HAS EMPTY SPACE (NO PARTIALS AND SOME FULLS)
                    pid = createNewPageAndGetId()                           #CREATE A NEW PAGE AND GET PAGE ID
                    bufferpool.pageDir[pid].write(value)                    #WRITE VALUE TO THE PAGE 
                    col-mem-partial[i].append(bufferpool.pageDir[pid])      #ADD PAGE TO PARTIAL PAGES IN MEMORY 
                    record_location[i] = (pid, offset)                      #SAVE THE LOCATION FOR THIS COLUMN 

        key_location[columns[0][0]] = [record_location]                     #INDEX THE LOCATION OF THE BASE RECORD EX: [((P-2, 0), (P-3, 3), (P-4, 2))]
        """

        record_location = ()
        for i, value in enumerate(columns[0]):                                      #LOOP THROUGH COLUMNS                            
            if len(self.bufferpool.colMemPartial[i]) > 0:
                page = self.bufferPool.colMemPartial[i][0]                         #RETRIEVE THE PAGE 
                page.write(value)                                                  #WRITE VALUE TO THE PAGE 
                record_location[i] = (page.pageID, page.numEntries - 1)            #SAVE THE LOCATION FOR THAT COLUMN 
                if not page.hasCapacity():                                         #IF THE PAGE IS FULL AFTER INSERT 
                    self.bufferPool.colMemFull[i].append(page)                     #MOVE IT TO FULL PAGES IN MEMORY 
                    self.bufferPool.colMemPartial[i].remove(page)                  #REMOVE IT FROM THE PARTIAL PAGES IN MEMORY 
            else:
                if not self.bufferpool.hasCapacityForColumn(i):
                    self.bufferpool.evict()
                    if len(self.bufferPool.colDiskPartial[i]) > 0:
                        page = self.bufferPool.getPageFromDisk(self.bufferPool.colDiskPartial[i])
                        page.write(value)
                        record_location[i] = (page.pageID, page.numEntries - 1)
                        if not page.hasCapacity():                  
                            self.bufferPool.colMemFull[i].append(page)            
                        else:                                       
                            self.bufferPool.colMemPartial[i].append(page)  

                    else:                                                   
                        pid = self.bufferPool.createNewPageAndGetIdForColumn(i)
                        page = self.bufferPool.colMemPartial[i][-1]
                        page.write(value)       # createNewPageAndGetIdForColumn create a page and appends it to column[i]                        
                        record_location[i] = (pid, page.numEntries - 1)     
                else:
                    pid = self.bufferPool.createNewPageAndGetIdForColumn(i)
                    page = self.bufferPool.colMemPartial[i][-1]
                    page.write(value)       # createNewPageAndGetIdForColumn create a page and appends it to column[i]                        
                    record_location[i] = (pid, page.numEntries - 1)     
        # rid = ()

        # col_page_ids = [""] * len(columns[0])
        # indexCols = [-1] * len(columns[0])
        # for i, value in enumerate(columns[0]): #Loop through the inserted columns 
            
        #     value = str(value)
            
        #     # if self.base_pages[i][self.bp_index[i]].has_capacity(len(value)): #For each column check if the current corresponding base page has empty room 
        #     #     self.base_pages[i][self.bp_index[i]].write(bytearray(value, "utf-8")) #Since there is room, write the column value to that base page 
        #     #     rid = rid + ((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1),) #Save the RID tuple for this column as (current bp_index, page.num_entries-1)
        #     #     #Subtract 1 from numEntries because the 10th element will be stored at position 9 in the page.rIndex 
        #     # else: 
        #     #     #If the base page is full 
        #     #     self.base_pages[i].append(Page()) #Create a new base page in that columns bp list 
        #     #     self.bp_index[i] += 1 #Increment the index of the current base page 
        #     #     self.base_pages[i][self.bp_index[i]].write(bytearray(value, "utf-8")) #Write the value to the new base page using the incremented index 
        #     #     rid = rid + ((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1),) #Save the RID tuple for this column as (incremented bp_index, page.num_entries-1)
        # # record = Record(rid, columns[0][0], list(columns[0])) #Create a record using the new rid, key(first column) and the columns 
        # # #print("TEST", columns[0])
        # # self.bp_directory[rid] = record #Map the RID to the physical record 
        
     
        # # self.key_rid[columns[0][0]] = [rid] #Map the key to the RID. Key: [rid, rid, rid]
        # # #Now we can do Key->RID->Record Useful for search 

        #     base_page = self.bufferPool.getPageById(self.base_pages[i][self.bp_index[i]])

        #     if base_page.has_capacity(len(value)): #For each column check if the current corresponding base page has empty room 
        #         base_page.write(bytearray(value, "utf-8")) #Since there is room, write the column value to that base page 
        #         rid = rid + ((self.bp_index[i],  base_page.numEntries - 1),) #Save the RID tuple for this column as (current bp_index, page.num_entries-1)                
        #         indexCols[i] = base_page.numEntries - 1
        #         #Subtract 1 from numEntries because the 10th element will be stored at position 9 in the page.rIndex 
        #     else: 
        #         #If the base page is full 
        #         self.base_pages[i].append(self.bufferPool.createNewPageAndGetId()) #Create a new base page in that columns bp list 
        #         self.bp_index[i] += 1 #Increment the index of the current base page 
        #         page = self.bufferPool.getPageById(self.base_pages[i][self.bp_index[i]])
        #         page.write(bytearray(value, "utf-8")) #Write the value to the new base page using the incremented index 
        #         rid = rid + ((self.bp_index[i],  page.numEntries - 1),) #Save the RID tuple for this column as (incremented bp_index, page.num_entries-1)
        #         indexCols[i] = page.numEntries - 1

        #     col_page_ids[i] = self.base_pages[i][self.bp_index[i]]

        # record = Record(rid, columns[0][0], col_page_ids, self.bufferPool, indexCols) #Create a record using the new rid, key(first column) and the columns 
        # #print("TEST", columns[0])
        # self.bp_directory[rid] = record #Map the RID to the physical record 
        
        # # print(record.pageCols)
        # self.key_rid[columns[0][0]] = [rid] #Map the key to the RID. Key: [rid, rid, rid]
        #Now we can do Key->RID->Record Useful for search 
 

    def delete(self, primary_key):
        base_record = self.bp_directory[self.key_rid[primary_key][0]]
        
        if base_record.rid in self.tp_directory: 
            for record in self.tp_directory[base_record.rid]:
                record.rid = -1 


       # for record in self.tp_directory[base_record.rid]: #Use the tp directory to loop through tail records of that base record 
            #record.rid = -1 #Invalidate each associated tail record 
        #base_record = self.bp_directory[self.key_rid[primary_key]]  #Find the physical record using the given key. 
        base_record.rid = -1 #Invalidate the base record rid 

    def update(self, primary_key, *columns):
        """
        latest_update_loc = key_location[primary_key][-1]   #GET THE LOCATION OF THE LATEST TAIL RECORD FOR THER KEY
        record_location = latest_update_loc                 #COPY THE LOCATION 
        for i,value in enumerate(columns):                  #LOOP THROUGH THE COLUMNS 
            if value != None:                               #IF COLUMN IS GETTING UPDATED 
                insert column logic                         #FOLLOW THE INSERT LOGIN FOR THAT COLUMN 
                record_location[i] = (page.pageID, offset)  #CHANGE RECORD LOCATION FOR THAT COLUMN TO UPDATED LOCATION 
        
        key_location[primary_key].append(record_location)   #APPEND TO LIST OF LOCATIONS, ONLY UPDATED COLUMNS WILL HAVE NEW LOCATION

        """
        base_record = self.bp_directory[self.key_rid[primary_key][0]]
        #base_record = Record(self.key_rid[primary_key][0], primary_key, self.bp_directory[self.key_rid[primary_key][0]].columns)
        latest_tail = None 
        if base_record.rid in self.tp_directory:
            latest_tail = self.tp_directory[base_record.rid][-1]
        else:
            latest_tail = base_record.copy()
        #tail_record = base_record
        tail_record = Record((), latest_tail.key, latest_tail.pageCols, latest_tail.bufferPool, latest_tail.indexCols)
        #tail_record.columns = columns
        
        for i,value in enumerate(columns):    
            if value != None:
                tail_page = self.bufferPool.getPageById(self.tail_pages[i][self.tp_index[i]])
                if tail_page.has_capacity(len(str(value))):
                    tail_page.write(bytearray(str(value), "utf-8"))
                    tail_record.rid = tail_record.rid + ((self.tp_index[i], tail_page.numEntries - 1),)
                else:
                    tail_page_id =  self.bufferPool.createNewPageAndGetId()
                    self.tail_pages[i].append(tail_page_id)
                    self.tp_index[i] += 1 

                    tail_page = self.bufferPool.getPageById(self.tail_pages[i][self.tp_index[i]])
                    tail_page.write(bytearray(str(value), "utf-8"))        
                    tail_record.rid = tail_record.rid + ((self.tp_index[i], tail_page.numEntries - 1),)
                #print("TEST: ", tail_record.columns)
                tail_record.indexCols[i] = tail_page.numEntries - 1
                tail_record.pageCols[i] = tail_page.pageID
                # tail_record.columns[i] = value

        if base_record.rid in self.tp_directory:
            self.tp_directory[base_record.rid].append(tail_record)
        else:
            self.tp_directory[base_record.rid] = [tail_record]
        
        
        #self.tp_directory[base_record.rid] += tail_record
        self.key_rid[primary_key].append(tail_record.rid)


        #     if value != None:
        #         if self.tail_pages[i][self.tp_index[i]].has_capacity(len(str(value))):
        #             self.tail_pages[i][self.tp_index[i]].write(bytearray(str(value), "utf-8"))
        #             tail_record.rid = tail_record.rid + ((self.tp_index[i], self.tail_pages[i][self.tp_index[i]].numEntries - 1),)
        #         else:
        #             self.tail_pages[i].append(Page())
        #             self.tp_index[i] += 1 
        #             self.tail_pages[i][self.tp_index[i]].write(bytearray(str(value), "utf-8"))        
        #             tail_record.rid = tail_record.rid + ((self.tp_index[i], self.tail_pages[i][self.tp_index[i]].numEntries - 1),)
        #         #print("TEST: ", tail_record.columns)
        #         tail_record.columns[i] = value

        # if base_record.rid in self.tp_directory:
        #     self.tp_directory[base_record.rid].append(tail_record)
        # else:
        #     self.tp_directory[base_record.rid] = [tail_record]
        
        
        # #self.tp_directory[base_record.rid] += tail_record
        # self.key_rid[primary_key].append(tail_record.rid)
       
    
    

    def __merge(self):
        print("merge is happening")

        """
        for key in key_loc: 
            slice key_loc[key] -> [loc1, loc2, loc3, loc4, loc5] -> [loc1, loc2, loc3, loc4],[loc5]
                for loc in [loc1, loc2, loc3, loc4, loc5]:
                    lock loc 
                    delete loc 

        
        
        """
        pass
    
 
