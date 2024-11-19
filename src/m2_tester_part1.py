from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

#print(f"[Tester.main] Testing Script ~~~Starto~~~~")
#print(f"[Tester.main] Creating new database instance...")
db = Database()
#print(f"[Tester.main] Empty database created: {db.tables}")
db.open('./CS451')
#print(f"[Tester.main] Path updated on open: {db.path}")
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
#print(f"[Tester.main] Create table...")
grades_table = db.create_table('Grades', 5, 0)
#print(f"[Tester.main] Created table! table name: {grades_table} - numColumns: {grades_table.num_columns} - PK index: {grades_table.key}")

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 10

seed(3562901)

for i in range(0, 100):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    if key == 92106429:
        print("INSERTING KEY: ", records[key])
    query.insert(*records[key])
keys = sorted(list(records.keys()))
#print("Insert finished")

# Check inserted records using select query
#print(f"\n\n[Tester.main] Starting select tests...")
for key in keys:
    #print(f"[Tester.main] Calling select on key: {key}")
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    #print(f"[Tester.main] Got record: {record.key}")
    error = False
    #print(f"[Tester.main] Checking record...")
    for i, column in enumerate(record.columns):
        #print(f"[Tester.main] Your Column: {column} - Expected: {records[key][i]}")
        if column != records[key][i]:
            #print(f"[Tester.main] ERROR")
            input(">> press enter to continue.")
            error = True
    if error:
        #print('select error on', key, ':', record, ', correct:', records[key])
        pass
#print(f"[Tester.main] Test complete.")
input(">>> Proceed to next test?")
#print(f"[Tester.main] ")

# x update on every column
#print(f"[Tester.main] Starting update tests")
for _ in range(number_of_updates):
    #print(f"[Tester.main] ")
    #print(f"[Tester.main] ")
    counter = 0
    for key in keys:
        #print(f"[Tester.main] ")
        updated_columns = [None, None, None, None, None]
        #print(f"[Tester.main] ")
        #print(f"[Tester.main] ")
        for i in range(2, grades_table.num_columns):
            #print(f"[Tester.main] ")
            # updated value
            #print(f"[Tester.main] ")
            value = randint(0, 20)
            #print(f"[Tester.main] ")
            updated_columns[i] = value
            #print(f"[Tester.main] ")
            # copy record to check
            original = records[key].copy()
            #print(f"[Tester.main] ")
            # update our test directory
            records[key][i] = value
            #print(f"[Tester.main] ")
            #print(f"[Tester.main] ")
            query.update(key, *updated_columns)
            
                
            #print(f"[Tester.main] ")
            
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            if key == 92106429:
                print("UPDATING KEY", record.columns)
                print("DESIRED UPDATE: ", records[key])
                
            #print(f"[Tester.main] ")
            error = False
            #print(f"[Tester.main] ")
            for j, column in enumerate(record.columns):
                #print(f"[Tester.main] ")
                if column != records[key][j]:
                    #print(f"[Tester.main] ")
                    #print(f"[Tester.main] ")
                    print(key, record.columns, column, records[key], records[key][j])
                    print(counter, len(keys))
                    input(">>> ERROR: Continue?")
                    error = True
                else:
                    #counter += 1 
                    #print("SUCCESSFUL UPDATE")
                    pass
            if error:
                #print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
                pass
            counter += 1

               
                #print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
#print("Update finished")

for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        #print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    
            pass
        # #print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
#print("Aggregate finished")
#print(type(keys[0]), keys[0])
print("TEST: ", query.select(92106429, 0, [1, 1, 1, 1, 1])[0].columns)
db.close()
