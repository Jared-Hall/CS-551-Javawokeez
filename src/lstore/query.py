from lstore.table import Table, Record
from lstore.index import Index

"""
select_version: 
return list of relative version record objects with search_key at search_key_index. only return columns with 1 
final = [] 
for page in memory-full, memory-partial[search_key_index]:              #LOOP THROUGH ALL PAGES IN MEMORY (PARTIAL AND FULL)
    if page.ValueIndex[search_key]:                                     #IF THE VALUE IS IN THE PAGE
        for key,ver in page.ValueIndex[search_key]:                     #LOOP THROUGH THE KEYS THAT HAVE THAT VALUE 
            relative = ver - len(key_locations[key]                     #ABSOLUTE VERSION - numVersions = relative version 
            if relative_version == relative:                            #IF RELATIVE VERSION OF RECORD IS DESIRED 
                columnsToAppend = []
                for i in numCols:                                       #LOOP THROUGH THE COLUMNS 
                    if projected_columns_index[i] == 1:                 #IF WE WANT TO RETURN THIS COLUMN 
                        data = key_locations[key][relative][i].read()   #READ THE VALUE FOR THE RELATIVE VERSION OF COLUMN
                        columnsToAppend.append(data)                    #APPEND THE COLUMN DATA TO THE COLUMNS TO RETURN 
                        record = Record(columnsToAppend)                #CREATE A RECORD OBJECT WITH DESIRED COLUMNS 
                final.append(record)                                    #APPEND TO LIST OF RECORD OBJECTS 

            
for page in disk-full, disk-partial[search_key_index]:                  #LOOP THROUGH ALL PAGES IN DISK (FULL OR PARTIAL)
    getPageByID(PID)                                                    #GET THE PAGE FROM DISK, THIS CALLS THE EVICTION POLICY 
    if page.ValueIndex[search_key]:                                     #IF THE PAGE CONTAINS THE VALUE 
        for key,ver in page.ValueIndex[search_key]:                     #LOOP THROUGH THE KEYS WITH THAT VALUE
            relative = ver - len(key_locations[key]                     #ABSOLUTE VERSION - numVersions = relative version 
            if relative_version == relative:                            #IF RELATIVE VERSION OF RECORD IS DESIRED 
                columnsToAppend = []
                for i in numCols:                                       #LOOP THROUGH COLUMNS
                    if projected_columns_index[i] == 1:                 #IF WE WANT TO RETURN THIS COLUMN 
                        data = key_locations[key][relative][i].read()   #READ THE DATA FOR RELATIVE VERSION OF THAT COLUMN
                        columnsToAppend.append(data)                    #APPEND IT 
                        record = Record(columnsToAppend)                #CREATE RECORD OBJECT
                final.append(record) 

return final                                                            #RETURN LIST OF RECORD OBJECTS 
"""


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
        if search_key not in self.table.index.pkl_index:
                return []
        #print(f"\n\n[Query.select] Select called. See input params below:")
        #print(f"[Query.select] search_key: {search_key} - search_key_index: {search_key_index} - projected_columns_index: {projected_columns_index}")
        #print(f"[Query.select] Calling select version on the latest record version")
        final = self.select_version(search_key, search_key_index, projected_columns_index, -1)
        #print(f"[Query.select] Returning: {final}") 
        return final
    
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
            if search_key not in self.table.index.pkl_index:
                return []
            #print(f"\n[Query.select_version] Select version called. See input params below:")
            #print(f"[Query.select_version] search_key: {search_key} - search_key_index: {search_key_index} - projected_columns_index: {projected_columns_index} - relative_version: {relative_version}")
            
            retVal = []
            
            #print(f"[Query.select_version] Checking if we are searching using the primary key...")
            if(search_key_index == self.table.key): #we are searching using the primary key 
                
                #print(f"[Query.select_version] Searching for records with the primary key: {search_key}")
                records = self.table.index.pkl_index[search_key]
                #print(f"[Query.select_version] Records found with this key: {record}")
                #print("RECORDS TO SELECT FROM: ", self.table.index.pkl_index[search_key])
                #print("RECORDS: ", search_key, records)
                for record in records[::-1]:
                    colToReturn = []
                    
                    #print(f"[Query.select_version] Fetching version the {relative_version} tail record: {record}")
                    #print(f"[Query.select_version] looping through the locations in the record...")
                    for location in record:
                        #print(f"[Query.select_version] Location: {location}. Getting page with PID: {location[0]}...")
                        page = self.table.bufferPool.getPage(location[0])
                        #print(f"[Query.select_version] Found page: {page.pageID}! Reading data from the page...")
                        data = page.read(location[1])
                        #print(f"[Query.select_version] Got data! raw: {data} - type: {type(data)} - int: {int(data)}! Appending to return record...")
                        colToReturn.append(int(data))
                        #print(f"[Query.select_version] data added. Continuing...")
                    
                    retVal.append(Record(search_key, self.FilterColumns(colToReturn, projected_columns_index)))
            if colToReturn[self.table.key] != search_key: 
                t = 0
            
            #print(f"[Query.select_version] Select version done! Returning: {retVal}")
            return retVal

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
        #if primary_key 
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
        """
        summationResult = 0
        keys = [start_range + i for i in range((end_range - start_range) + 1)] 
        for key in keys:
            location = key_location[key][-1]
            summationResult += location[aggregate_column_index].read() 
        
        return summationResult 
        
        """
    
        summationResult = 0
        keys = [start_range + i for i in range((end_range - start_range) + 1)]
        for key in keys:
            if key in self.table.index.pkl_index:
                location = self.table.index.pkl_index[key][-1]
                summationResult += int(self.table.bufferPool.getPage(location[aggregate_column_index][0], aggregate_column_index).read(location[aggregate_column_index][1]))
        return summationResult
    
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
        """
        summationResult = 0
        keys = [start_range + i for i in range((end_range - start_range) + 1)] 
        for key in keys:
            location = key_location[key][relative_version]
            summationResult += location[aggregate_column_index].read() 
        
        return summationResult 
        
        """
        summationResult = 0
        keys = [start_range + i for i in range((end_range - start_range) + 1)] 
        for key in keys:
            location = self.key_rid[key][relative_version]
            summationResult += self.bufferpool.getPageByID(location[aggregate_column_index][0]).read() 
        return summationResult 

    def GetVersionColumnValue(self, search_key, relative_version, columnIndex):
        try:
            record_locations = self.table.key_rid[search_key] 
            
            #step-01: [br]   
            records = [self.table.bp_directory[record_locations[0]]]
            # [br, te1, tr2, ....]
            if(record_locations[0] in self.table.tp_directory): #if there are tail records
                records += self.table.tp_directory[record_locations[0]]

            if(relative_version == 0):
                return records[-1].getColumnValue(columnIndex)
            elif(abs(relative_version) >= len(records)):
                return records[0].getColumnValue(columnIndex)
            else:
                return records[relative_version - 1].getColumnValue(columnIndex)
        except:
            return False
        pass

    
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
