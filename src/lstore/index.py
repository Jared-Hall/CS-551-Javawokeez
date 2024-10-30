"""
Documentation for the page class.
Author: Nabil Abdel-Rahman nabilabdel-rahman@outlook.com, Jared Hall jhall10@uoregon.edu
Description:
    This file contains our implementation of the core storage data structure for our L-Store database.
    The description of this data structure is as follows:

Notes:
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""
from lib.bTree import *

class Index:
    """
    The index class.
    This allows for fast retreval of records stored in the database for query and updating.
    Structure:
    (index) -> tree of records, where the leafs are (rid, pointer).
    (index) -> HashMap of columns for the table: {cID:<pointer_to_column>}
    """

    def __init__(self, num_columns):
        """
        Description: the constructor for the index.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """        
        #Step-01: Build the record index (b+ tree)
        self.tree = BPlusTree()
        #step-02: Build Hashmap of columns from table.num_columns
        self.column_indices = {}


    def _getRecord(self, rid):
        """
        Description: returns the index for the specified record 
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        return self.tree.query(rid)

    def _getRangeRecord(self, startID, endID):
        """
        Description: returns the locations of all records between "startID" and endID.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        return self.tree.rangeQuery(startID, endID)
    
    def _addColumn(self, cID, columnPointer):
        """
        Description: Inserts the column ID along with it's key
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        self.column_indices[cID] = columnPointer


    def _createIndex(self, recordPointer):
        """
        Description: Inserts the record into the b+ Tree and creates a UID, which is then returned
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        return self.tree.insert()

    def _deleteIndex(self, rid):
        """
        Description: remove an entry from the index keyed by rid. boolean function returns True if successful, else false if key doesn't exist.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        return self.tree.delete(rid)

    def _dropColumn(self, cID):
        """
        Description: drop a column from the HashMap and delete every instance of that column in every record (Note:  may not actually need this )
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        del self.column_indices[cID]


    def locate(self, columnID, value):
        """
        Description: returns the location of all records with the given value on column by columnID
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        if self.column_indices[columnID] is None:
            return []
        return self.column_indices[columnID].get(value, [])


    def locate_range(self, begin, end, cID):
        """
        Description: Returns the RIDs of all records with values in column specified by cID between "begin" and "end"
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        if self.column_indices[cID] is None:
            return []
        
        rids = []
        for value in range(begin, end + 1):
            rids.extend(self.column_indices[cID].get(value, []))
        
        return rids

#=======================================================================================================================
    def create_index(self, cID):
        """
        Description: <desc>
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        if self.column_indices[cID] is None:
            self.column_indices[cID] = {}

    def drop_index(self, cID):
        """
        Description: Drop index of specific column
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        if self.column_indices[cID] is not None:
            self.column_indices[cID] = None 