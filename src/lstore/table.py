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

        record_location = [[]]*len(columns[0])
        for i, value in enumerate(columns[0]): 
            index = None   
           
            
            #self.index.vk_index[i][self.key][value] = {0:[columns[0][0]]}                                 #LOOP THROUGH COLUMNS                            
            if len(self.bufferPool.colMemPartial[i]) > 0:
                page = self.bufferPool.colMemPartial[i][0]  
                                               #RETRIEVE THE PAGE 
                index = page.write(value)
                                                              #WRITE VALUE TO THE PAGE 
                record_location[i] = (page.pageID, index)                #SAVE THE LOCATION FOR THAT COLUMN 
                data = page.read(index)   
                print("DATA FOR COL: ", i, data, page.pageID, index)            
                if not page.hasCapacity():                                         #IF THE PAGE IS FULL AFTER INSERT 
                    self.bufferPool.colMemFull[i].append(page)                     #MOVE IT TO FULL PAGES IN MEMORY 
                    self.bufferPool.colMemPartial[i].remove(page)                  #REMOVE IT FROM THE PARTIAL PAGES IN MEMORY 
            else: #no partials in memory
                if(len(self.bufferPool.colDiskPartial[i]) > 0):
                    page = self.bufferPool.getPage(self.bufferPool.colDiskPartial[0].pageID, i)
                    index = page.write(value)
                    record_location[i] = (page.pageID, index)   
                    data = page.read(index)   
                    print("DATA FOR COL: ", i, data, page.pageID, index)                                         #SAVE THE LOCATION FOR THAT COLUMN 
                    if not page.hasCapacity():                                         #IF THE PAGE IS FULL AFTER INSERT 
                        self.bufferPool.colMemFull[i].append(page)                     #MOVE IT TO FULL PAGES IN MEMORY 
                        self.bufferPool.colMemPartial[i].remove(page)
                else:
                    page = self.bufferPool.getPage(columnIdx=i)
                    index = page.write(value)
                     
                    record_location[i] = (page.pageID, index)                #SAVE THE LOCATION FOR THAT COLUMN 
                    data = page.read(index)   
                    print("DATA FOR COL: ", i, data, page.pageID, index)            
                    if not page.hasCapacity():                                         #IF THE PAGE IS FULL AFTER INSERT 
                        self.bufferPool.colMemFull[i].append(page)                     #MOVE IT TO FULL PAGES IN MEMORY 
                        self.bufferPool.colMemPartial[i].remove(page)
        
        print("TUPLE: ", [tuple([tuple(x) for x in record_location])])
        print("FIRST COLUMN: ", columns[0][0])
         
        self.index.pkl_index[columns[0][0]] = [tuple([tuple(x) for x in record_location])]

        print("UPDATED: ", self.index.pkl_index[columns[0][0]])
        
        
          
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
        latest_update_loc = self.key_rid[primary_key][-1] 
        page = self.bufferPool.getPageByID(latest_update_loc)
        newColumns = []
        for i in range(self.num_columns):
            page = self.bufferPool.getPageByID(latest_update_loc[i][0]) 
            newColumns.append(page.read(latest_update_loc[i][1]))
        
        for i, value in enumerate(columns):
            if value != None:
                newColumns[i] = value 
            
        self.insert(newColumns) 
    
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
        print("merge is happening")

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
    
 
