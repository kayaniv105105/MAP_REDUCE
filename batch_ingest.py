import happybase

# create connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)


# open connection to perform operations
def open_connection():
    connection.open()


# close the opened connection
def close_connection():
    connection.close()


# list all tables in Hbase
def list_tables():
    print("fetching all table")
    open_connection()
    tables = connection.tables()
    close_connection()
    print("all tables fetched")
    return tables


# create a table by passing name and column family as a parameter
def create_table(name, cf):
    print("creating table " + name)
    tables = list_tables()
    if name not in tables:
        open_connection()
        connection.create_table(name, cf)
        close_connection()
        print("table created")
    else:
        print("table already present")


# get the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table


def batch_insert_data(filename, tablename, columnsfm):
    print("starting batch insert of " + filename)
    file = open(filename, 'r')
    table = get_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=50000) as a:
        for line in file:
            if i != 0 or i != 1:
                w = line.strip().split(",")
                a.put(w[1] + w[2], {columnsfm+':VendorID': str(w[0]), columnsfm+':tpep_pickup_datetime': str(w[1]),
                                    columnsfm+':tpep_dropoff_datetime': str(w[2]),
                                    columnsfm+':passenger_count': str(w[3]), columnsfm+':trip_distance': str(w[4]),
                                    columnsfm+':RatecodeID': str(w[5]), columnsfm+':store_and_fwd_flag': str(w[6]),
                                    columnsfm+':PULocationID': str(w[7]), columnsfm+':DOLocationID': str(w[8]),
                                    columnsfm+':payment_type': str(w[9]), columnsfm+':fare_amount': str(w[10]),
                                    columnsfm+':extra': str(w[11]), columnsfm+':mta_tax': str(w[12]), columnsfm+':tip_amount': str(w[13]),
                                    columnsfm+':tolls_amount': str(w[14]), columnsfm+':improvement_surcharge': str(w[15]),
                                    columnsfm+':total_amount': str(w[16]),
                                    columnsfm+':congestion_surcharge': str(w[17]), columnsfm+':airport_fee': str(w[18])})

            i += 1

    file.close()
    print("batch insert done")
    close_connection()

if __name__ == '__main__':
    print("Enter table name ")
    tablename = input()
    print("Enter columns family name")
    columnsfm = input()
    create_table(tablename, {columnsfm: dict(max_versions=5)})
    print('Enter your input file name for batch uplaod')
    filename =  input()
    batch_insert_data(filename, tablename,columnsfm)
    print('Batch upload completed')

