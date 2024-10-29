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
        self.base_pages = [[Page()] for i in range(num_columns)]
        self.tail_pages = [[Page()] for i in range(num_columns)]
        self.bp_directory = {}
        self.tp_directory = {}
        self.bp_index = [0] * num_columns
        self.tp_index = [0] * num_columns

        pass

    def insert(self, *columns):
        rid = []
        for i, value in enumerate(columns):
            if self.base_pages[i][self.bp_index[i]].has_capacity():
                self.base_pages[i][self.bp_index[i]].write(value)
                rid.append((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1)) 
            else:
                self.base_pages[i].append(Page()) 
                self.bp_index[i] += 1 
                self.base_pages[i][self.bp_index[i]].write(value)
                rid.append((self.bp_index[i],  self.base_pages[i][self.bp_index[i]].numEntries - 1)) 
        record = Record(rid, columns[0], columns)
        self.bp_directory[rid] = record 
        self.index.add(rid, columns[0])


       
    
    

    def __merge(self):
        print("merge is happening")
        pass
    
 
