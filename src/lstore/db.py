"""
Documentation for the db(database) class.
Author: Nabil Abdel-Rahman nabilabdel-rahman@outlook.com, Jack O'Donnell Jodonnel@uoregon.edu 
Description:
    This file contains the implementation for the database. 
    The database handles creating, deleting, and retrieving tables 

"""

from lstore.table import Table



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
        table = Table(name, num_columns, key_index)
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
