from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        err = self.table.delete(primary_key)
        return err == None
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # TODO: Add schema encoding column to the list of columns 
        self.table.insert(columns)
        

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        record_locations = self.table.key_rid[search_key]  
        #print("LOCS: ", record_locations) 
        #for rid in record_locations:
            #print(rid in self.table.tp_directory)
        columnsToReturn = []
        if record_locations[0] in self.table.tp_directory:
            for record in self.table.tp_directory[record_locations[0]]:
                #columnsToReturn.append(self.FilterColumns(record.columns, projected_columns_index))
                columnsToReturn.insert(0, record)
            columnsToReturn.append(self.table.bp_directory[record_locations[0]])

        else:
            
            #columnsToReturn.append(self.FilterColumns(self.table.bp_directory[record_locations[0]].columns, projected_columns_index)) 
            columnsToReturn.append(self.table.bp_directory[record_locations[0]])
            #print(record)
        
        return columnsToReturn
        """
        Select all records where column[search_key_index] = search_key        
        record_locations = self.table.index.locate(search_key, search_key_index)
       
         Assuming record_locations is an array of rids:
        
        for rid in record_locations:            
            record = self.table.bp_directory[rid]
            if record != None:                
                columnsToReturn.append(self.FilterColumns(record.columns, projected_columns_index))

        return columnsToReturn
        """
        
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        record_locations = self.table.key_rid[search_key] 
        
        #step-01: [br]   
        records = [self.table.bp_directory[record_locations[0]]]
        # [br, te1, tr2, ....]
        if(record_locations[0] in self.table.tp_directory): #if there are tail records
            records += self.table.tp_directory[record_locations[0]]

        if(relative_version == 0):
            return [records[-1]]
        elif(abs(relative_version) >= len(records)):
            return [records[0]]
        else:
            return [records[relative_version - 1]]


    def FilterColumns(self, columns, projected_columns_index):
        columnsToReturn = []
        for i, value in enumerate(columns):
            if projected_columns_index[i] == 1:
                columnsToReturn.append(value)
        return columnsToReturn
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        self.table.update(primary_key, *columns)
       

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        summationResult = 0
        keys = [start_range + i for i in range((end_range - start_range) + 1)] 
        record_locations = []
        for key in keys: 
            if key in self.table.key_rid:
                record_locations.append(self.table.key_rid[key][0])
        #record_locations = [self.table.key_rid[key][0] for key in keys]
        for rid in record_locations:
            if rid in self.table.tp_directory: 
                summationResult += self.table.tp_directory[rid][-1].columns[aggregate_column_index] 
            else:
                #print(self.table.bp_directory[rid].columns, self.table.bp_directory[rid].columns[0])
                summationResult += self.table.bp_directory[rid].columns[aggregate_column_index]
        if len(record_locations) == 0:
            return False
        return summationResult
      
        """
        record_locations = self.table.index.locate_range(start_range, end_range, 0)
        for rid in record_locations:
            latestRecord  = self.table.bp_directory[rid]
            if rid in self.table.tp_directory and len(self.table.tp_directory[rid]) > 0: 
                latestRecord = self.table.tp_directory[rid][len(self.table.tp_directory[rid]) - 1]
            
            summationResult += latestRecord.columns[aggregate_column_index]
        return summationResult
        """

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        summationResult = 0

        record_locations = self.table.index.locate_range(start_range, end_range, 0)
        
        if len(record_locations) == 0:
            return False
        
     
        
        for rid in record_locations:
            relativeRecord  = self.table.bp_directory[rid]
            if rid not in self.table.tp_directory or len(self.table.tp_directory[rid]) < relative_version:
                return False
            
            relativeRecord = self.table.tp_directory[rid][len(self.table.tp_directory[rid]) - relative_version - 1]
            summationResult += relativeRecord.columns[aggregate_column_index]

        return summationResult

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
