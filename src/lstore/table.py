"""
Documentation for the table(database) class.
Author: Jack O'Donnell Jodonnel@uoregon.edu 
Description:
    This file contains the implementation for the table. 

"""
from lstore.index import Index
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, key, columns):
       # self.rid = rid
        self.key = key

        #self.bufferPool = bufferPool
        #self.indexCols = indexCols

        self.columns = columns
        #Change columns to hold list of page ids
        # self.columns = columns
        self.pageCols = columns

        self.indirection = None



class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferPool, path):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.path = f"{path}/{name}/"
        self.page_directory = {}
        self.index = Index(num_columns)
        self.bufferPool = bufferPool
        self.bufferPool.path = self.path

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
                if not bufferPool.hasCapacityForColumn(i):          #IF bufferPool IS FULL (OF FULL PAGES)
                    bufferPool.evict()                              #EVICT A PAGE BASED ON POLICY NOW THERE IS AN EMPTY SPACE IN MEMORY  
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
                        bufferPool.pageDir[pid].write(value)                #WRITE VALUE TO THE PAGE 
                        col-mem-partial[i].append(bufferPool.pageDir[pid])  #ADD THE PAGE TO PARTIAL PAGES IN MEMORY 
                        record_location[i] = (pid, offset)                  #SAVE THE LOCATION FOR THAT COLUMN 

                else:                                                       #IF THE bufferPool HAS EMPTY SPACE (NO PARTIALS AND SOME FULLS)
                    pid = createNewPageAndGetId()                           #CREATE A NEW PAGE AND GET PAGE ID
                    bufferPool.pageDir[pid].write(value)                    #WRITE VALUE TO THE PAGE 
                    col-mem-partial[i].append(bufferPool.pageDir[pid])      #ADD PAGE TO PARTIAL PAGES IN MEMORY 
                    record_location[i] = (pid, offset)                      #SAVE THE LOCATION FOR THIS COLUMN 

        key_location[columns[0][0]] = [record_location]                     #INDEX THE LOCATION OF THE BASE RECORD EX: [((P-2, 0), (P-3, 3), (P-4, 2))]
        """
        
        
        print("\n\n========== Insert ==========")
        print(f"[Table.insert] columns: {columns}")
        record_location = [[]]*self.num_columns
        print(f"[Table.insert] record_location: {record_location}")
        print(f"[Table.insert] Looping through columns[0]: {columns[0]}")
        for i, value in enumerate(columns[0]): 
            print(f"[Table.insert] column i: {i} value: {value}")
            index = None   
            print(f"[Table.insert] Check if there are partial pages in bufferpool")
            print(f"[Table.insert] {len(self.bufferPool.colMemPartial[i]) > 0}")
            if(len(self.bufferPool.colMemPartial[i]) > 0):
                print(f"[Table.insert] There are partial pages. Fetching Page")
                page = self.bufferPool.colMemPartial[i][0]
                print(f"[Table.insert] Got a partial page from memory. Page: {page.pageID}")
                print(f"[Table.insert] Calling page.write to write value ({value}) to page...")
                index = page.write(value)
                print(f"[Table.insert] Write Complete! index: {index} - number of open spots left: {len(page.availableOffsets)}")
                print(f"[Table.insert] Wrote: {page.data[index: (index+8)]}")
                print(f"[Table.insert] <debug>: reading that data from the page...") 
                data = page.read(index)
                print(f"[Table.insert] <debug>: Returned data: {data} {page.pageID} {index}")   
                print(f"[Table.insert] Adding (PID, idx): {(page.pageID)}, {index}") 
                record_location[i] = (page.pageID, index)
                print(f"[Table.insert] updated record_location: {record_location[i]}")
                print(f"[Table.insert] Check if the page has capacity.")
                print(f"[Table.insert]     page.capacity: {page.hasCapacity()} - ")            
                if not page.hasCapacity():
                    print(f"[Table.insert] The page has no open spots. Appending to full...")                                         #IF THE PAGE IS FULL AFTER INSERT 
                    self.bufferPool.colMemFull[i].append(page)
                    print(f"[Table.insert] Removing page from partial mem...")  
                    self.bufferPool.colMemPartial[i].remove(page)
                    print(f"[Table.insert] removed Page.") 
                    index = list(self.bufferPool.pageDirectory[page.pageID]) 
                    index[0] = 1
                    self.bufferPool.pageDirectory[page.pageID] = tuple(index)
            else: #no partials in memory
                print(f"[Table.insert] No partials in memory. Checking if there is a partial page on disk...")
                if(len(self.bufferPool.colDiskPartial[i]) > 0):
                    print(f"[Table.insert] Disk has a partial! Calling BP.getPage with PID: {page.pageID} - column: {i}")
                    page = self.bufferPool.getPage(self.bufferPool.colDiskPartial[0].pageID, i)
                    print(f"[Table.insert] Returned page ({page.pageID}) - writing {value}...")
                    index = page.write(value)
                    print(f"[Table.insert] <debug>: reading that data at index {index} from the page...") 
                    data = page.read(index)
                    print(f"[Table.insert] <debug>: Returned data: {data} {page.pageID} {index}")   
                    print(f"[Table.insert] Adding location (PID, idx): ({(page.pageID)}, {index})") 
                    record_location[i] = (page.pageID, index)
                    print(f"[Table.insert] updated record_location: {record_location[i]}")    
                    print(f"[Table.insert] Checking if the page is full...")                                          
                    if(not page.hasCapacity()):
                        print(f"[Table.insert] The page has no open spots. Appending to full...")                                         #IF THE PAGE IS FULL AFTER INSERT 
                        self.bufferPool.colMemFull[i].append(page)
                        print(f"[Table.insert] Removing page from partial mem...")  
                        self.bufferPool.colMemPartial[i].remove(page)
                        print(f"[Table.insert] removed Page.")
                        index = list(self.bufferPool.pageDirectory[page.pageID]) 
                        index[0] = 1
                        self.bufferPool.pageDirectory[page.pageID] = tuple(index)
                else:
                    print(f"[Table.insert] No partials available. Creating new page for column: {i}")
                    page = self.bufferPool.getPage(columnIdx=i)
                    print(f"[Table.insert] Returned page ({page.pageID}) - writing {value}...")
                    index = page.write(value)
                    print(f"[Table.insert] <debug>: reading that data at index {index} from the page...") 
                    data = page.read(index)
                    print(f"[Table.insert] <debug>: Returned data: {data} {page.pageID} {index}")   
                    print(f"[Table.insert] Adding location (PID, idx): ({(page.pageID)}, {index})") 
                    record_location[i] = (page.pageID, index)
                    print(f"[Table.insert] updated record_location: {record_location[i]}. Checking if the page has capacity.")            
                    if not page.hasCapacity():
                        print(f"[Table.insert] Page is full appending to the full list") 
                        self.bufferPool.colMemFull[i].append(page)
                        print(f"[Table.insert] Removing page from partial list") 
                        self.bufferPool.colMemPartial[i].remove(page)
                        print(f"[Table.insert] Page Removed")
                        index = list(self.bufferPool.pageDirectory[page.pageID]) 
                        index[0] = 1
                        self.bufferPool.pageDirectory[page.pageID] = tuple(index)
                    print(f"[Table.insert] Done creating page!")
                print(f"[Table.insert] Done eith fetching page from memory or creating page")
            print(f"[Table.insert] Finished insert!")     
        print(f"[Table.insert] Updating PKL index with new record. ")
        record_location = [(tuple([tuple(x) for x in record_location]))]
        print(f"[Table.insert] Record to insert: {record_location} primary key: {columns[0][0]}")
        if(int(columns[0][0]) in self.index.pkl_index):
            print(f"[Table.insert] Base record exists. Something is wrong since you are trying to insert a dup.")
            self.index.pkl_index[int(columns[0][0])].append(record_location[0])
        else:
            self.index.pkl_index[int(columns[0][0])] = record_location
        print(f"[Table.insert] Complete!")
        print("============================")
        
        
          
    def delete(self, primary_key):
        for loc in self.index.pkl_index[primary_key]:
             #loc = ((pid, idx) () () ())
             for i, pid in enumerate(loc):
                page = self.bufferPool.getPageByID(pid[0])
                data = page.read(pid[1]) 
                page.remove(pid[1]) 
                for version in self.index.vk_index[i][data]: 
                    if primary_key in self.index.vk_index[i][data][version]:
                        self.index.vk_index[i][data][version].remove(primary_key)
      
        del self.index.pkl_index[primary_key] 

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
        latest_update_loc = self.index.pkl_index[primary_key][-1] 
        newColumns = []
        for i in range(self.num_columns):
            page = self.bufferPool.getPage(latest_update_loc[i][0], i) 
            data = page.read(latest_update_loc[i][1])
            newColumns.append(data)
        
        for i, value in enumerate(columns):
            if value != None:
                newColumns[i] = value 

        #print("OLD, NEW", columns, newColumns)    
        self.insert(tuple(newColumns)) 
    
    def save(self): 
        """
        save mem pages to disk: 

        """
        for page in self.bufferPool.getMemPages():
            self.bufferPool.savePage(page[0].pageID)

        with open(f"{self.path}{self.name}.meta", "w") as file:
            file.write(f"{self.num_columns}\n")
            file.write(f"{self.key}\n")

            for key, value in self.bufferPool.page_directory:
                file.write(f"{key}:{value}\n")
            
            file.write("Index") 
            file.write(str(self.index))
    
    def load(self, bufferPool): 
        with open(f"{self.path}{self.name}.meta", "r") as file:
            self.num_columns = file.readline() 
            self.key = file.readline() 
            while line != "Index": 
                line = file.readline()
                key, value = line.split(":") 
            
                tuple_of_integers = tuple([int(x) for x in value[:-1].strip('()').split(',')])
                self.bufferPool.pageDirectory[key] = tuple_of_integers 
                colIdx = tuple_of_integers[2]
                if tuple_of_integers[0]: 
                    self.bufferPool.colDiskFull[colIdx].append(key) 
                else:
                    self.bufferPool.colDiskPartial[colIdx].append(key)
                
            pkl_str = file.readline()   
            vk_str = file.readline() 
            self.index.load(pkl_str, vk_str)

    def __merge(self):
        #print("merge is happening")

        """
        for key in key_loc: 
            slice key_loc[key] -> [loc1, loc2, loc3, loc4, loc5] -> [loc1, loc2, loc3, loc4],[loc5]
                for loc in [loc1, loc2, loc3, loc4, loc5]:
                    lock loc 
                    delete loc 
        """
        for key in self.key_rid: 
            deletedUpdates = self.key_rid[key][:-1] 
            newLocs = [self.key_rid[key][-1]] 
            self.key_rid[key] =  newLocs 
            for loc in deletedUpdates:
                self.delete_version(loc)
        pass
    
 
