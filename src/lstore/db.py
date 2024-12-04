"""
Documentation for the db(database) class.
Author: Jared Hall (jhall10@uoregon.edu), Nabil Abdel-Rahman nabilabdel-rahman@outlook.com, Jack O'Donnell Jodonnel@uoregon.edu 
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
from lstore.bufferpool import BufferPool
from os import remove
import logging

from lstore.setupLogging import *

class Database():

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log = setupLogger(False, "DEBUG", self.log, 4)
        self.tables = {} #Store name and tables as key:value
        self.path = './storage'

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
        for name, table in self.tables.items():
            #Step-01: Write all memory to disk and close the table
            table.save()

    
    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        bufferpool = BufferPool(num_columns, 10*num_columns)
        table = Table(name, num_columns, key_index, bufferpool, self.path)
        self.tables[name] = table #Store the table
        return table

    def drop_table(self, name):
        """
        Deletes the specified table
        """
        if name in self.tables: 
            del self.tables[name]
        else:
             raise Exception("Table not in database ")
        pass

    def get_table(self, name):
        """
        Returns table with the passed name
        """
        numColumns = 0
        with open(f"{self.path}/{name}/{name}.meta", "r") as file:
            numColumns = int(file.readline().strip('\n'))
            key = int(file.readline())
        table = self.create_table(name, numColumns, key)
        table.load()
        self.tables[name] = table
        return table 