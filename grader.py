from database import *

import psycopg2
import traceback

from urllib.parse import urlparse, uses_netloc
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']


uses_netloc.append("postgres")
url = urlparse(connection_string)

conn = psycopg2.connect(database=url.path[1:],
  user=url.username,
  password=url.password,
  host=url.hostname,
  port=url.port
)

product_table = "products"
customer_table = 'customers'
order_table = 'orders'

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS " + order_table)
cur.execute("DROP TABLE IF EXISTS " + product_table)
cur.execute("DROP TABLE IF EXISTS " + customer_table)

conn.commit()



score = 100

initialize()

########
# Insert a product - 10 points
########
upsert_product({'name': 'Salt', 'price': 0.99})
cur = conn.cursor()
cur.execute("select id, name, price from " + product_table)
if cur.rowcount != 1:
    print('-10\tInsert product fails, expected 1 record after first insert, got ' , cur.rowcount)
    score-=10
else:
    salt = cur.fetchone()
    if salt[1] != 'Salt' or salt[2] != 0.99:
        print('-10\tInsert product fails, inserted record does not have expected fields. ')
        score-=10

upsert_product({'name': 'Pepper', 'price': 2.99})
conn.commit()


########
# Insert a customer - 10 points
########
try:
    upsert_customer({'firstName': 'Peter', 'lastName': 'Mercer', 'street': '505 Ramapo Valley Road', 'city': 'Mahwah', 'state': 'NJ', 'zip': '07430'})
    cur = conn.cursor()
    cur.execute("select lastName, city from " + customer_table)
    if cur.rowcount != 1:
        print('-10\tInsert customer fails, expected 1 record after first insert, got ' , cur.rowcount)
        score-=10
    else:
        a = cur.fetchone()
        if a[0] != 'Mercer' or a[1] != 'Mahwah':
            print('-10\tInsert customer fails, inserted record does not have expected fields. ')
            score-=10
    conn.commit()
    upsert_customer({'firstName': 'Phil', 'lastName': 'Murphy', 'street': '125 West State Street', 'city': 'Trenton', 'state': 'NJ', 'zip': '08601'})
except:
    print('-10\tInsert customer fails, iexception. ')
    traceback.print_exc()
    score-=10

#######
# Insert an order - 10 points
#######
try:
    cur = conn.cursor()
    cur.execute("select id from " + customer_table + " where firstName = 'Peter'")
    peter = cur.fetchone()
    cur.execute("select id from " + product_table + " where name = 'Salt'")
    salt = cur.fetchone()
    cur.execute("select id from " + product_table + " where name = 'Pepper'")
    pepper = cur.fetchone()

    cur.execute("select id from " + customer_table + " where firstName = 'Phil'")
    phil = cur.fetchone()

    upsert_order({'customerId': peter[0], 'productId': salt[0], 'date':'2018-06-30'})

    cur.execute('select id, customerId, productId from ' + order_table)
    if cur.rowcount != 1:
        print('-10\tInsert order fails, expected 1 record after first insert, got ' ,cur.rowcount)
        score-=10
    else:
        a = cur.fetchone()
        if a[1] != peter[0] or a[2] != salt[0]:
            print('-10\tInsert order fails, inserted record does not have expected fields. ')
            print(a)
            score-=10
    conn.commit()
except:
    print('-10\tInsert order fails with exception')
    traceback.print_exc()
    score-=10

########
# Get customer(s) - 10 Points
########
try:

    customers = list(get_customers())
    if len(customers) != 2:
        print('-10\tGet customers failed, expected 2, got ', len(customers))
        score-=10

    pres = get_customer(peter[0])
    if pres == None or pres['firstName'] != 'Peter':
        print('-10\tGet customers failed, did not return expected record, got')
        score-=10   
except:
    print('-10\tGet customers failed - exception or incorrect data format')
    traceback.print_exc()


    score-=10   
########
# Get products(s) - 10 Points
########.
try:
    products = list(get_products())
    if len(products) != 2:
        print('-10\tGet products failed, expected 2, got ', len(products))
        score-=10

    salty = get_product(salt[0])
    if salty == None or salty['name'] != 'Salt':
        print('-10\tGet products failed, did not return expected record, got')
        score-=10    
except:
    print('-10\tGet products failed - exception or incorrect data format')
    traceback.print_exc()
    score-=10  

########
# Get order(s) - 20 Points
########
try:
    upsert_order({'customerId': peter[0], 'productId': pepper[0], 'date':'1979-03-18'})
    orders = list(get_orders())
    if len(orders) != 2:
        print('-30\tGet orders failed, expected 2, got ', len(orders))
        score-=30

    elif 'product' not in orders[0]:
        print('-30\tGet orders failed, no product attached')
        score-=30
    elif 'customer' not in orders[0]:
        print('-30\tGet orders failed, no customer attached')
        score-=30
    elif orders[0]['customer']['firstName'] != 'Peter':
        print('-30\tGet orders failed, incorrect customer attached')
        score-=30

except:
    print('-30\tGet orders fails with exception')
    traceback.print_exc()

upsert_order({'customerId': phil[0], 'productId': salt[0], 'date':'2018-05-30'})

########
# Sales report - 10 points
########
sr = sales_report()
try:
    salt_report = [p for p in sr if p['id'] is salt[0]]
    if len(sr) != 2:
        print('-10\tSales report has', len(sr), 'entries instead of 2')
        score-=10
    elif len(salt_report) < 1:
        print('-10\tSales report has', len(sr), 'entries instead of 2')
        score-=10
    else:
        salt_report = salt_report[0]
        if salt_report['gross_revenue'] != 1.98:
            print('-10\tSales report contains incorrect data')
            print(salt_report)
            score-=10   
except:
    print('-10\tSales report throws exception')
    traceback.print_exc()
    score-=10

########
# Delete customer - 10 points
########
try:
    delete_customer(peter[0])
    cur.execute("select id from " + customer_table + " where firstName = 'Peter'")
    peter = cur.fetchone()
    if peter != None:
        print('-10\tCustomer delete did not work.')
        score-=10
except:
    print('-10\tCustomer delete fails - should use cascade.')
    traceback.print_exc()
    score-=10

print('Final Score:  ' + str(score))
