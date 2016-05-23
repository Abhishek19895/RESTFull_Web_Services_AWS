__author__ = 'abhisheksingh29895'

import  os,  sys,  httplib,  ast
#loading all the packages
sys.path.append('/anaconda/lib/python2.7/site-packages')
sys.path.append('/usr/local/Cellar/openssl/1.0.2c/lib/')
from flask import Flask, request, jsonify


#SERVER = 'localhost:5000'

SERVER = '52.33.94.217:5000'


def get_counts_per_rating():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/beauty/overall/counts')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




def get_counts_per_salescategory():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/meta_beauty/SalesCategory/counts')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out



def get_counts_per_year():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/beauty/year/count')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




def get_names_top_20_customers():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/beauty/last5years/top20/names')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




def get_proportion_mosthelpful_ratings_per_year():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/beauty/last5years/100perc_helpful/proportion')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




def get_counts_per_price_bucket():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/meta_beauty/price_bucket/counts')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




def get_avg_price_per_sales_category():
    out  =  dict()
    h  =  httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/meta_beauty/Sales_Category/Avg.Price')
    resp  =  h.getresponse()
    out  =  resp.read()
    return  out




if __name__ == '__main__':
    print "************************************************"
    print "test of my flask app running at ", SERVER
    print "created by Abhishek Singh"
    print "************************************************"
    print " "
    print "******** counts per Overall rating **********"
    print get_counts_per_rating()
    print " "
    print "******** counts per Sales Category **********"
    print get_counts_per_salescategory()
    print " "
    print "******** counts per year **********"
    print get_counts_per_year()
    print " "
    print "******** Get names of Top 20 Customers **********"
    print get_names_top_20_customers()
    print " "
    print "******** Get proportion of 100% helpful ratings every year **********"
    print get_proportion_mosthelpful_ratings_per_year()
    print " "
    print "******** Get number of items in each price bucket **********"
    print get_counts_per_price_bucket()
    print " "
    print "******** Get Average price per Sales category **********"
    print get_avg_price_per_sales_category()