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

-----------------------------------------------------------------------------------------------------------------------                       
    Index (VK): Values -> Keys
    Description: The Value-Key (VK) index is a level 2 column-wise index that maps values in a column to the
                 primary keys of records with that value in this column. It is also version specific.
                 The VK index is a level 2 index sine the BP index points to an entry in this index.
    Format:
    [
        {
            <Value>: {
                        <Version> : [<Primary Key>, ...],
                    },
        },
    ,
    ...
    ]
    

    k1 -> to delete
    PK[k1]->record(loc1, loc2)
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
        self.vk_index = [{}] * numColumns 
    
    def __repr__(self):
        """
        Format:
        PKL
        {
            <primary key> : [((<PID>, <idx>), (<PID>, <idx>), ...), ... ]
        }

        pkl Rep:
        pk:PID-idx#PID-idx#PID-idx|PID-idx#PID-idx#PID-idx,pk:...\n

        VK:
         <value>:<version>#<key>#<key>#...#<key>|<version>...,<value>...@<value>.....\n"

        """
        #Step-01: Build the PKL string representation
        pkl = []
        for key in self.pkl_index: #key -> [rec, rec, rec] | rec -> (c1, c2, ...) | c -> (PID, idx)
            records = [] #[rec, rec, red, ...]
            for record in self.pkl_index[key]:
                record = '#'.join([f"{page[0]}-{page[1]}" for page in record])
                records.append(record)
            pkl.append(f"{key}:{'|'.join(records)}")
        pkl = ','.join(pkl)

        #Step-02: Build vkl
        vk = []
        for columnWiseIndex in self.vk_index:
            values = []
            for value in columnWiseIndex:
                versions = []
                for version in columnWiseIndex[value]:
                    keys = '#'.join(columnWiseIndex[value][version])
                    versions.append(f"{version}#{keys}")
                values.append(f"{value}:{'|'.join(versions)}")
            vk.append(f"{','.join(values)}")
        vk = '@'.join(vk)

        return f"{pkl}\n{vk}\n"
    
    def load(self, pklRep, vkRep):
        """
        Format:
        pkl Rep:
        pk:PID-idx#PID-idx#PID-idx|PID-idx#PID-idx#PID-idx,pk:...\n

        VK:
         <value>:<version>#<key>#<key>#...#<key>|<version>...,<value>...@<value>.....\n"
        """
        

        #Step-01: Build pkl index from string rep
        pklRep = pklRep[:-1].split(',') #  [pk:PID-idx#PID-idx#PID-idx|PID-idx#PID-idx#PID-idx, pk:...]
        for pkRep in pklRep: # pkRep -> pk:PID-idx#PID-idx#PID-idx|PID-idx#PID-idx#PID-idx
            pkRep = pkRep.split(':') #pkRep -> [pk, PID-idx#PID-idx#PID-idx|PID-idx#PID-idx#PID-idx]
            key = pkRep[0]
            self.pkl_index[key] = []
            records = pkRep[1].split('|') # [PID-idx#PID-idx#PID-idx, PID-idx#PID-idx#PID-idx, ...]
            for recRep in records: #record -> PID-idx#PID-idx#PID-idx
                recRep = recRep.split('#') #[PID-idx, PID-idx, PID-idx]
                record = []
                for page in recRep:
                    page = page.split('-') #[PID, idx]
                    page = tuple(page[0], int(page[1]))
                    record.append(page)
                record = tuple(record)
                self.pkl_index[key].append(record)
        
        #Step-02: Build vk from string rep
        vkRep = vkRep[:-1].split('@') #[<value>:<version>#<key>#<key>#...#<key>|<version>...,<value>..., ...]
        self.vk_index = []
        for colRep in vkRep: #colRep -> <value>:<version>#<key>#<key>#...#<key>|<version>...,<value>...
            column = {}
            for valRep in colRep.split(','): #valRep -> [<value>:<version>#<key>#<key>#...#<key>|<version>..., ...]
                valRep = valRep.split(':') #[<value>, <version>#<key>#<key>#...#<key>|<version>...]
                value = valRep[0]
                column[value] = {}
                for verRep in valRep[1].split('|'): #[<version>#<key>#<key>#...#<key>, <version>...]
                    verRep = verRep.split('#') # [<version>, <key>, <key>, ..., <key>]
                    column[int(verRep[0])] = verRep[1:]
            self.vk_index.append(column)
        
    def getLoc(self, primaryKey, recordIdx):
        return self.pkl_index[primaryKey][recordIdx]
    
    def setLoc(self, key, recordIdx, record):
        self.pkl_index[key].insert(record, recordIdx)

    def getIndexByValue(self, colIndx, value, version):
        if(value in self.vk_index[colIndx]):
            if(version in self.vk_index[colIndx][value]):
                return self.vk_index[colIndx][value][version]
        return False

            


            

                
            

    
    

       
        
if(__name__=="__main__"):
    index = Index()
