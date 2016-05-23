
#Loading all the needed packages
import  os,  sys,  random
sys.path.append('/anaconda/lib/python2.7/site-packages')
sys.path.append('/usr/local/Cellar/openssl/1.0.2c/lib/')
import  json,  ast,  psycopg2,  psycopg2.extras,  numpy  as  np


# DSN location of the AWS - RDS instance
DB_DSN = "host=msan1.cd62umwwi1nc.us-west-2.rds.amazonaws.com  dbname=yourdb  user=dbuser  password=dbpassword"


def  fix_json(input_file,  output_file):
    """
    Function to correct bad json files
    #Reading the file & writing it simultaneously
    """
    fr  =  open(input_file)  ;  fw  =  open(output_file, "w")
    for  line  in  fr:
        json_dat  =  json.dumps(ast.literal_eval(line))
        dict_dat  =  json.loads(json_dat)
        json.dump(dict_dat,  fw)
        fw.write("\n")
    fw.close()  ;  fr.close()



def  extract_beauty(filename):
    """
    Reading the the reviews JSON file and storing it as a list
    Input: JSON FILE OUTPUT: list of Tuples (10000)
    """
    data  =  []
    with  open(filename)  as  f:
        for  line  in  f:
            data.append(json.loads(line))
    data_list  =  []
    #transforming the json files into a list of tuples
    for i  in  data:
        asin  =  i.get('asin',None)  ;  name  =  i.get('reviewerName',None)
        time  =  i.get('reviewTime',None)  ;  overall  =  int(i.get('overall',None))
        help  =  i.get('helpful',[])  ;  helpful  =  None
        if  help:
            if  help[1]>0:
                helpful  =  help[0]*1.0/help[1]
            else:
                helpful  =  0
        #removing unicodes from the various elements
        if  asin:
            asin  =  asin.encode('utf-8')
        if  name:
            name  =  name.encode('utf-8')
        if  time:
            time  =  time.encode('utf-8')
        tup  =  (asin,  helpful, name,  overall,  time)
        data_list.append(tup)
    #Now that the list is ready, randomly sample 10,000 tuples
    # from this list and push it to the database
    data  =  random.sample(data_list,  10000)
    #returning the ouput
    return  data




def  extract_meta_beauty(filename):
    """
    Reading the the corrected meta data JSON file and storing it as a list
    Input: JSON FILE OUTPUT: list of Tuples (10000)
    """
    data1  =  []  ;  data_list  =  []
    with  open(filename)  as  f:
        for  line  in  f:
            data1.append(json.loads(line))
    for i  in  data1:
        asin  =  i.get('asin',None) ;  title  =  i.get('title',None)
        price  =  i.get('price',None)  ;  salesRank  =  json.dumps(i.get('salesRank',None))
        categ  =  i.get('categories',{})  ;  categories  =  {}
        if  categ:
            categ  =  categ[0]
            for  a  in range(len(categ)):
                categories[a]  =  categ[a]
        categories  =  json.dumps(categories)
        if  price:
            price  =  int(price)
        if  asin:
            asin  =  asin.encode('utf-8')
        if  title:
            title  =  title.encode('utf-8')
        tup  =  (asin,  salesRank,  price,  categories,  title)
        data_list.append(tup)
    #Now that the list is ready, randomly sample 10,000 tuples
    # from this list and push it to the database
    data  =  random.sample(data_list,  10000)
    #returning the ouput
    return  data



def drop_meta_table():
    """
    Function to drop the table storing meta data, if it exists
    """
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur = conn.cursor()
        cur.execute("drop table if exists meta_beauty")
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message




def drop_reviews_table():
    """
    Function to drop the table storing reviews data, if it exists
    """
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur = conn.cursor()
        cur.execute("drop table if exists beauty")
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message



def  create_meta_table():
    """
    Function to create a table for Meta Data, if it does not exists
    """
    try:
        sql  =  "create table meta_beauty(asin TEXT primary key" \
                ",salesRank JSON" \
                ",price FLOAT" \
                ",categories JSON" \
                ",title TEXT)"
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message




def create_reviews_table():
    """
    Function to create a table for the reviews Data, if it does not exists
    """
    try:
        sql  =  "create table beauty(asin TEXT" \
                ",helpful FLOAT" \
                ",name TEXT" \
                ",overall INTEGER"\
                ",Review_date DATE)"
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message




def  insert_reviews_data(data):
    """
    Functions to insert reviews data from lists into RDS
    """
    try:
        sql  =  "INSERT INTO beauty VALUES(%s,%s,%s,%s,%s)"
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message



def  insert_meta_data(data):
    """
    Functions to insert meta data from lists into RDS
    """
    try:
        sql  =  "INSERT INTO meta_beauty VALUES(%s,%s,%s,%s,%s)"
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor()
        cur.executemany(sql, data)
        conn.commit()
        cur.close()
        conn.close()
    except  psycopg2.Error  as  e:
       print  e.message



if __name__ == '__main__':
    """
    Running the main function and printing the outputs
    """
    print "******* Correcting data **********"
    fix_json("meta_Beauty.json","fixed_Beauty.json")
    print "******* transforming data **********"
    beauty_list  =  extract_beauty("reviews_Beauty.json")
    beauty_meta_list  =  extract_meta_beauty("fixed_Beauty.json")
    print "******* dropping meta table **********"
    drop_meta_table()
    print "******* dropping reviews table **********"
    drop_reviews_table()
    print "******* creating meta table **********"
    create_meta_table()
    print "******* creating review table **********"
    create_reviews_table()
    print "******* inserting 10000 rows of meta data **********"
    insert_meta_data(beauty_meta_list)
    print "******* inserting  10000 rows of reviews data **********"
    insert_reviews_data(beauty_list)
    print "******* Deleting extra files **********"
    os.remove("fixed_Beauty.json")
