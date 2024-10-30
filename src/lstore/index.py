"""
Documentation for the index class.
Author: Nabil Abdel-Rahman <email>, Jared Hall jhall10@uoregon.edu
Description:
    This file contains our implementation of the core storage data structure for our L-Store database.
    The description of this data structure is as follows:

Notes:
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:
    """
    The index class.
    This allows for fast retreval of records stored in the database for query and updating.
    Structure:
    (index) -> tree of records, where the leafs are (rid, pointer).
    (index) -> HashMap of columns for the table: {cID:<pointer_to_column>}
    """

    def __init__(self, table):
        """
        Description: the constructor for the index.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        #Step-01: Build the record index (b+ tree)

        #step-02: Build Hashmap of columns from table.num_columns
        pass


    def _getIndex(self, rid):
        """
        Description: returns the index for the specified record 
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass

    def _getRangeIndex(self, startID, endID):
        """
        Description: returns the locations of all records between "startID" and endID.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass     

    def _createIndex(self, recordPointer):
        """
        Description: Inserts the record into the b+ Tree and creates a UID, which is then returned
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass

    def _deleteIndex(self, rid):
        """
        Description: remove an entry from the index keyed by rid. boolean function returns True if successful, else false if key doesn't exist.
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass

    def _dropColumn(cID):
        """
        Description: drop a column from the HashMap and delete every instance of that column in every record (Note:  may not actually need this )
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass


    def locate(self, columnID, value):
        """
        Description: returns the location of all records with the given value on column by columnID
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass


    def locate_range(self, begin, end, cID):
        """
        Description: Returns the RIDs of all records with values in column specified by cID between "begin" and "end"
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass

#=======================================================================================================================
    def create_index(self, cID):
        """
        Description: <desc>
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass

    def drop_index(self, cID):
        """
        Description: Drop index of specific column
        Inputs: 
            varName <type>: <description>
        Outputs:
            output <type>: <description>
        """
        pass  