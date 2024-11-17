from lstore.page import Page
"""
Documentation for the page class.
Author:Jared Hall - jhall10@uoregon.edu, Nabil Abdel-Rahman - nabilabdel-rahman@outlook.com, 
Description:
    This file contains our implementation of the core storage data structure for our L-Store database.
    The description of this data structure is as follows:

Notes:
1. Completely rewrote this class for m2. We decieded to completely forgo the use of b+-trees entirely
   in favour of maps which are much faster at small scales.
2. Index objects are table specific. So each table has it's own index.

Indexes:
-----------------------------------------------------------------------------------------------------------------------
    Index (PKL): Primary Keys -> Physical Locations (records)
    Description: This index maps primary keys to the physical locations in the page where the data is stored.
                 Takes the form of a dictionary of lists where the key is the PK and the value is the record in tuple format.
                 Tuple format: (col_1, col_2, ..., col_|columns|), where each column (e.g., col_1) is represented by a tuple (<PID>, <idx>).
                 <PID> is ID of the specific page where the data is stored, <idx> is the index of the data in the data array
                 The list is in sorted format: [br, tr, tr, tr, .., latest_tr]
    Format:
    {
        <primary key> : [((<PID>, <idx>), (<PID>, <idx>), ...), ... ]
    }
-----------------------------------------------------------------------------------------------------------------------    Index (Bufferpool):
    Index (BP): Column -> Pages 
    Description: The Buffer Pool (BP) index holds references to every page in the DB, both in memory and on disc.
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
-----------------------------------------------------------------------------------------------------------------------                       
    Index (VK): Values -> Keys
    Description: The Value-Key (VK) index is a level 2 column-wise index that maps values in a column to the
                 primary keys of records with that value in this column. It is also version specific.
                 The VK index is a level 2 index sine the BP index points to an entry in this index.
    Format:
    {
        <Value>: {
                    <Version> : [<Primary Key>, ...],
                 },
    }
-----------------------------------------------------------------------------------------------------------------------                       
"""

class Index:
    """
    Description: The index class allows the DB to quickly and efficently get references to objects/records in the DB.
                 The main focus was to optimise speed and efficiency over space.
    
    

    """

    def __init__(self, numColumns):
        """
        Description: the constructor for the index.
        Inputs: 
            The index is empty initially and only gets populated as things get created or loaded.
        """
        self.pkl_index = {}
        self.vk_index = {}

        #Build the BP index
        self.bp_index = [[[], [], [], []] for x in range(numColumns)]
    
    

       
        
if(__name__=="__main__"):
    index = Index()
