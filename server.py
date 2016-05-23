__author__ = 'abhisheksingh29895'



import  os,  sys
#loading all the packages
sys.path.append('/anaconda/lib/python2.7/site-packages')
sys.path.append('/usr/local/Cellar/openssl/1.0.2c/lib/')
import  json,  ast,  psycopg2,  psycopg2.extras
from  collections  import  defaultdict
from flask import Flask, request, jsonify



# DSN location of the AWS - RDS instance
DB_DSN = "host=msan1.cd62umwwi1nc.us-west-2.rds.amazonaws.com  dbname=yourdb  user=dbuser  password=dbpassword"


#Setting up the application
app  =  Flask(__name__)


#Welcome :)
@app.route('/')
def default():
    output = dict()
    # nothing is going on here
    output['message'] = 'Welcome Michael, I would be showing you a part of my web services'
    return jsonify(output)



@app.route('/beauty/overall/counts')
def get_counts_per_rating():
    """
    Getting frequency for overall ratings from the reviews data
    :return: a dict of key = [OVERALL RATING] and value = [Number of Items]
    """
    out = dict()
    sql = "select overall, count(1) cnt from beauty group by overall order by 1 DESC"
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
           print item
           out[item['overall']]  =  item['cnt']
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return  jsonify(out)




@app.route('/meta_beauty/SalesCategory/counts')
def get_counts_per_salescategory():
    """
    Getting Counts of Items sold for each Category from the meta data table
    :return: a dict of key = [CATEGORY] and value = [Number of Items]
    """
    out  =  dict()
    sql  =  """select Category,count(1) as cnt
          from (select json_object_keys(salesrank) as Category
          from meta_beauty
          where salesrank::text !='null')A
          group by Category
          order by 2 DESC"""
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['category']]  =  item['cnt']
    except  psycopg2.Error  as  e:
        print e.message
    else:
        cur.close()
        conn.close()
    return jsonify(out)




@app.route('/beauty/year/count')
def get_counts_per_year():
    """
    Counts of Items bought per year in chronological order
    :return: a dict of key = [YEAR] and value = [Number of Items]
    """
    out = dict()
    sql = "select to_char(review_date,'YYYY')::INT as Year,count(1) cnt " \
          "from beauty " \
          "group by to_char(review_date,'YYYY')::INT " \
          "order by 1 DESC"
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['year']]  =  item['cnt']
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return  jsonify(out)




@app.route('/beauty/last5years/top20/names')
def get_names_top_20_customers():
    """
    Names of Top 20 customers who purchased the highest number of
    items in the last 5 years (In Descending order)
    :return: a dict of key = [Customer Names] and value = [Number of Items]
    """
    out = dict()
    sql  =  "select name,count(1) Items_purchased " \
            "from beauty " \
            "where to_char(review_date,'YYYY')::INT BETWEEN 2010 and 2014 " \
            "and name not in ('Amazon Customer','Pen Name','') " \
            "group by name " \
            "order by 2 DESC " \
            "limit 20"
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['name']]  =  item['items_purchased']
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return  jsonify(out)




@app.route('/beauty/last5years/100perc_helpful/proportion')
def get_proportion_mosthelpful_ratings_per_year():
    """
    Proportion of products each year with 100% helpful rating during past 5 years
    :return: a dict of key = [YEAR] and value = [Proportion of Items]
    """
    out  =  dict()
    sql  =  """select to_char(review_date,'YYYY')::INT as Year
            ,round(SUM(case when helpful = 1 THEN 1 ELSE 0 END)*1.0/count(1),2) All_5
          from beauty
          where to_char(review_date,'YYYY')::INT between 2010 and 2014
          group by to_char(review_date,'YYYY')::INT
          order by 1 DESC"""
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['year']]  =  float(item['all_5'])
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return jsonify(out)




@app.route('/meta_beauty/price_bucket/counts')
def get_counts_per_price_bucket():
    """
    Getting counts of products per price bracket from the meta data table
    :return: a dict of key = [Price-Bucket] and value = [Count of Items]
    """
    out  =  dict()
    sql  =  "select " \
          "case when price >=50 then 'a. >=50' " \
          "when price between 25 and 49 then 'b. 25-49' " \
          "when price between 10 and 24 then 'c. 10-24' " \
          "when price between 5 and 9 then 'd. 5-9' " \
          "when price < 5 then 'e. < 5' " \
          "END as Price_Bucket, count(1) cnt " \
          "from meta_beauty " \
          "where price is not NULL " \
          "group by case when price >=50 then 'a. >=50' " \
          "when price between 25 and 49 then 'b. 25-49' " \
          "when price between 10 and 24 then 'c. 10-24' " \
          "when price between 5 and 9 then 'd. 5-9' " \
          "when price < 5 then 'e. < 5' " \
          "END order by 1"
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['price_bucket']]  =  item['cnt']
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return  jsonify(out)




@app.route('/meta_beauty/Sales_Category/Avg.Price')
def get_avg_price_per_sales_category():
    """
    Getting Average price of products for each Sales category in a Descending order
    :return: a dict of key = Sales_Category and value = AVG.PRICE of Items
    """
    out  =  dict()
    sql  =  "select Sales_Category,avg(price)::INT as AVG_price " \
          "from (select json_object_keys(salesrank) as Sales_Category,price " \
          "from meta_beauty " \
          "where salesrank::text!='null')A " \
          "group by Sales_Category " \
          "order by 2 DESC"
    try:
        conn  =  psycopg2.connect(dsn  =  DB_DSN)
        cur  =  conn.cursor(cursor_factory  =  psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rs  =  cur.fetchall()
        for  item  in  rs:
            out[item['sales_category']]  =  item['avg_price']
    except  psycopg2.Error  as  e:
        print  e.message
    else:
        cur.close()
        conn.close()
    return  jsonify(out)



if __name__ == "__main__":
    app.debug = True # only have this on for debugging!
    app.run(host='0.0.0.0', port = 5000) # need this to access from the outside world!
