from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.base_pages = [[Page()] for i in range(num_columns)] #List of List [[Base Pages for column 1], [Base Pages for column 2], [Base Pages for column 3]]
        self.tail_pages = [[Page()] for i in range(num_columns)] #List of List [Tail Pages for column 1], [Tail Pages for column 2], [Tail Pages for column 3]]
        self.bp_directory = {} # Maps RID -> Base Record 
        self.tp_directory = {} # Maps BaseRecord.rid -> [TailRecord, TailRecord, Tailrecord]
        self.bp_index = [0] * num_columns #List of stored index for base pages. [0, 1, 2] access self.base_pages[i][self.bp_index[i]] 
        self.tp_index = [0] * num_columns #List of stored index for tail pages. [0, 1, 2] access self.tail_pages[i][self.bp_index[i]] 
        self.key_rid = {} #Maps key->rid can use to find all rids with that key 

        pass

    def insert(self, *columns):
        """
        RID is stored as a list of tuples. each tuple represents a base page location and offset. 
        Each tuple in the list corresponds to a column/page for that column. 
        For example, RID = [(0,2), (3,2), (1,1)] -> The second column value is stored in self.base_pages[1][3] and is the 2nd element in that page 
        self.bp_index is used to track the index of the current base page as it fills 
        """
        rid = [] 
        for i, value in enumerate(columns): #Loop through the inserted columns 
            if self.base_pages[i][self.bp_index[i]].has_capacity(): #For each column check if the current corresponding base page has empty room 
                self.base_pages[i][self.bp_index[i]].write(value) #Since there is room, write the column value to that base page 
                rid.append((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1)) #Save the RID tuple for this column as (current bp_index, page.num_entries-1)
                #Subtract 1 from numEntries because the 10th element will be stored at position 9 in the page.rIndex 
            else: 
                #If the base page is full 
                self.base_pages[i].append(Page()) #Create a new base page in that columns bp list 
                self.bp_index[i] += 1 #Increment the index of the current base page 
                self.base_pages[i][self.bp_index[i]].write(value) #Write the value to the new base page using the incremented index 
                rid.append((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1)) #Save the RID tuple for this column as (incremented bp_index, page.num_entries-1)
        record = Record(rid, columns[0], columns) #Create a record using the new rid, key(first column) and the columns 
        self.bp_directory[rid] = record #Map the RID to the physical record 
        self.key_rid[columns[0]].append(rid)  #Map the key to the RID. Key: [rid, rid, rid]
        #Now we can do Key->RID->Record Useful for search 
 

    def delete(self, primary_key):
        for record in self.tp_directory[base_record.rid]: #Use the tp directory to loop through tail records of that base record 
            record.rid = -1 #Invalidate each associated tail record 
        base_record = self.bp_directory[self.key_rid[primary_key]]  #Find the physical record using the given key. 
        base_record.rid = -1 #Invalidate the base record rid 

    def update(self, primary_key, *columns):
        
        base_record = self.bp_directory[self.key_rid[primary_key]]
        tail_record = base_record
        tail_record.columns = columns 
        for i,value in enumerate(columns):
            if value != None:
                if self.tail_pages[i][self.tp_index[i]].has_capacity():
                    self.tail_pages[i][self.tp_index[i]].write(value)
                    tail_record.rid[i] = (self.tp_index[i], self.tail_pages[i][self.tp_index[i]].numEntries - 1)
                else:
                    self.tail_pages[i].append(Page())
                    self.tp_index[i] += 1 
                    self.tail_pages[i][self.tp_index[i]].write(value)        
                    tail_record.rid[i] = (self.tp_index[i], self.tail_pages[i][self.tp_index[i]].numEntries - 1)
        self.tp_directory[base_record.rid].append(tail_record)
        self.key_rid[primary_key].append(self.t)
       
    
    

    def __merge(self):
        print("merge is happening")
        pass
    
 
