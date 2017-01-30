#!/usr/bin/python2.7
#
# Interface for the assignement
#
import sys
import psycopg2

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'Ratings'
MAX_RATING = 5.0
MIN_RATING = 0.0
RANGE_TABLE_NAME="range_part"
ROUND_ROBIN_TABLE="rrobin_part"
INPUT_FILE_PATH='E:/Python Projects/Proj1/ratings.dat'
RANGE_PARTITIONS= 0
RROBIN_PARTITIONS=0

METADATA_TABLE="metadata_table"


def getopenconnection(user='postgres', password='cgsasu123', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")




def loadratings( ratingstablename, ratingsfilepath, openconnection):

    with open(ratingsfilepath,'r') as doc:
        docs = doc.read()
    docs = docs.replace('::',':')
    with open(ratingsfilepath,'w') as doc:
        doc.write(docs)

    curs=openconnection.cursor()
    curs.execute("DROP TABLE if EXISTS " + ratingstablename)
    curs.execute("CREATE TABLE "+ratingstablename + "(UserID INT, MovieID INT,Rating REAL, temp varchar(10)); ")


    movie_data=open(ratingsfilepath,'r')
    print("before copy")
    curs.copy_from(movie_data, ratingstablename, sep=':', columns=('UserID', 'MovieID', 'Rating', 'temp'))
    curs.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN temp")




    curs.execute("DROP TABLE if EXISTS " + METADATA_TABLE)

    curs.execute("CREATE TABLE " + METADATA_TABLE + "(table_type INT,num_partitions INT, partition_range REAL,next_table INT); ")
    curs.execute("INSERT INTO " + METADATA_TABLE + " VALUES (%d,%d,%f,%d)" % (1,0,0,0)) # 1 is range_part. 2 is rrobin_part
    curs.execute("INSERT INTO " + METADATA_TABLE + " VALUES (%s,%d,%f,%d)" % (2, 0, 0, 0))
    print("meta")


    curs.close()



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        curs = openconnection.cursor()
        RANGE_PARTITIONS=numberofpartitions
        partition_range = MAX_RATING/numberofpartitions
        for i in range(0, numberofpartitions):
            minVal = i * partition_range
            maxVal = minVal + partition_range
            tableName = RANGE_TABLE_NAME + str(i)
            curs.execute("DROP TABLE IF EXISTS " + tableName)
            # print(RANGE_TABLE_NAME+"_"+"%s" %i)
            curs.execute("CREATE TABLE "+tableName+"(UserID INT, MovieID INT,Rating REAL)")
            print("after create")
            if i==0:
                curs.execute("INSERT INTO "+tableName + " SELECT * from "+ratingstablename + " where Rating >= "+str(minVal)+" AND Rating <= "+str(maxVal))
            #print("sdcas")
            else:
                curs.execute("INSERT INTO " + tableName + " SELECT * from " + ratingstablename + " where Rating > "+str(minVal)+" AND Rating <= "+str(maxVal))
            curs.execute("Update "+METADATA_TABLE+" SET num_partitions = "+str(numberofpartitions)+", partition_range ="+str(partition_range)+" WHERE table_type=%d;"%(1))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if curs is not None:
            curs.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

        curs=openconnection.cursor()
        RROBIN_PARTITIONS=numberofpartitions
        for i in range(0,numberofpartitions):
            tableName = ROUND_ROBIN_TABLE + `i`
            curs.execute("DROP TABLE IF EXISTS " + tableName)
            curs.execute("CREATE TABLE "+tableName+"(UserID INT, MovieID INT,Rating REAL)")
        curs.execute("select * from "+ratingstablename)
        file = curs.fetchall()
        print("after fetch")
        # print(row)

        i=0
        for data in file:
            tableName= ROUND_ROBIN_TABLE + `i`
            # print(tableName)
            curs.execute("INSERT INTO "+tableName+" VALUES (%s, %s, %s)" % (data[0], data[1], data[2]))
            i=i+1
            # print(i)
            i=i%numberofpartitions
            # print("i="+str(i))
        curs.execute("Update "+METADATA_TABLE+" SET num_partitions ="+str(numberofpartitions)+", next_table="+str(i)+"WHERE table_type=%d;"%(2))


        curs.close()




def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    curs=openconnection.cursor()
    try:
        print "inside try"
        curs.execute("Select  num_partitions, next_table from "+METADATA_TABLE+" where table_type=%d ;"%(2))
        meta_data=curs.fetchone()
        num_partitions=meta_data[0]
        print(num_partitions)
        print meta_data[0]
        next_table=meta_data[1]
        print next_table



        curs.execute("Insert into "+ROUND_ROBIN_TABLE+str(next_table)+" values ( %s,%s,%s)" %(userid,itemid,rating))
        next_table+=1
        next_table=next_table%num_partitions
        curs.execute("Update "+METADATA_TABLE+" SET next_table ="+str(next_table)+" where table_type=%d ;"%(2))


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if curs is not None:
            curs.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    curs = openconnection.cursor()
    try:
        print "inside try"
        print rating
        curs.execute("Select  num_partitions, partition_range from " + METADATA_TABLE + " where table_type=%d ;" % (1))
        meta_data = curs.fetchone()
        num_partitions = meta_data[0]

        print(num_partitions)
        #print meta_data[0]
        partition_range = meta_data[1]
        print partition_range
        temp=[]
        for i in range(0,5.0):
            temp.append(i*partition_range)
        for i in range(5):
            if(temp[i]>rating):
                x=i
        x=x-1

        curs.execute("Insert into " + ROUND_ROBIN_TABLE + str(x) + " values ( %s,%s,%s)" % (userid, itemid, rating))

        #next_table = next_table % num_partitions
        #curs.execute("Update " + METADATA_TABLE + " SET next_table =" + str(next_table) + " where table_type=%d ;" % (2))




    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if curs is not None:
            curs.close()


def delete_partitions(openconnection):
    curs = openconnection.cursor()
    curs.execute("SELECT num_partitions, table_type FROM "+METADATA_TABLE)
    rows = curs.fetchall()
    for row in rows:
        num_partitions = row[0]
        t_type=row[1]
        if t_type==1:
            table_type=RANGE_TABLE_NAME
        else:
            table_type=ROUND_ROBIN_TABLE
        for i in range(0,num_partitions):
                table_name=table_type+'i'
                curs.execute("DROP TABLE if EXISTS "+table_name)
    curs.execute("DROP TABLE IF EXISTS "+ METADATA_TABLE)
    curs.close()





def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)
            comm = sys.argv[1]
            print(sys.argv[0])

            loadratings(RATINGS_TABLE, 'E:/Python Projects/Proj1/Tester/test_data.dat', con)
            rangepartition(RATINGS_TABLE, 5, con)
            roundrobinpartition(RATINGS_TABLE, 4, con)
            rangeinsert(RATINGS_TABLE, 10, 12, 3, con)

            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
