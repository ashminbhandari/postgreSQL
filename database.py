from urllib.parse import urlparse, uses_netloc
import configparser
import psycopg2
import psycopg2.extras

# Create connection
config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

uses_netloc.append("postgres")
url = urlparse(connection_string)

conn = psycopg2.connect(database=url.path[1:],
                        user=url.username,
                        password=url.password,
                        host=url.hostname,
                        port=url.port)


# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your PostgreSQL database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection and create
    # any tables that should exist, but do not yet exist in the database.
    curs = conn.cursor()
    curs.execute("DROP TABLE IF EXISTS orders")
    curs.execute("DROP TABLE IF EXISTS products")
    curs.execute("DROP TABLE IF EXISTS customers")

    conn.commit()

    curs.execute("create table customers("
                 "id SERIAL PRIMARY KEY, "
                 "firstName text, "
                 "lastName text, "
                 "street text, "
                 "city text, "
                 "state text, "
                 "zip int, "
                 "CONSTRAINT same_customer UNIQUE(firstName, lastName, street, city, state, zip))")
    curs.execute("create table products("
                 "id SERIAL PRIMARY KEY,"
                 "name text UNIQUE,"
                 "price real)")
    curs.execute("create table orders("
                 "id SERIAL PRIMARY KEY,"
                 "customerId int,"
                 "productId int,"
                 "date text,"
                 "FOREIGN KEY (customerId) REFERENCES customers(id) ON DELETE SET NULL ON UPDATE CASCADE,"
                 "FOREIGN KEY (productId) REFERENCES products(id) ON DELETE SET NULL ON UPDATE CASCADE)")
    conn.commit()


def get_customers():
    curr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    curr.execute('select * from customers')
    retList = []
    for customer in curr:
        toRet = {'id': customer[0],
               'firstName': customer[1],
               'lastName': customer[2],
               'street': customer[3],
               'city': customer[4],
               'state': customer[5],
               'zip': customer[6]}
        retList.append(toRet)

    return retList

def get_customer(id):
    curr = conn.cursor()
    curr.execute('select * from customers where customers.id=(%s)', (id,))
    customer = curr.fetchone()
    retList = {'id': customer[0],
               'firstName': customer[1],
               'lastName': customer[2],
               'street': customer[3],
               'city': customer[4],
               'state': customer[5],
               'zip': customer[6]}
    return retList

def upsert_customer(customer):
    curr = conn.cursor()
    curr.execute('insert into customers(firstName, lastName, street, city, state, zip) values(%s, %s, %s, %s, %s, %s) on conflict on constraint same_customer do nothing',
                 (customer['firstName'], customer['lastName'], customer['street'], customer['city'], customer['state'], customer['zip']))
    conn.commit()


def delete_customer(id):
    curr = conn.cursor()
    curr.execute('delete from customers where customers.id=(%s)', (id,))
    conn.commit()


def get_products():
    curr = conn.cursor()
    curr.execute('select * from products')
    return curr


def get_product(id):
    curr = conn.cursor()
    curr.execute('select * from products where products.id=(%s)', (id,))
    one = curr.fetchone()
    product = {'id': one[0], 'name': one[1], 'price': one[2]}
    return product


def upsert_product(product):
    curr = conn.cursor()
    curr.execute(
        'insert into products(name, price) values(%s, %s) on conflict (name) do update set price = excluded.price',
        (product['name'], product['price']))
    conn.commit()


def delete_product(id):
    curr = conn.cursor()
    curr.execute('delete from products where products.id=(%s)', (id,))
    conn.commit()


def get_orders():
    curr = conn.cursor()
    curr.execute('select * from orders')
    ordersList = []
    for each in curr:
        order = {'id': each[0], 'customerId': each[1], 'productId': each[2], 'date': each[3],
                 'customer': get_customer(int(each[1])), 'product': get_product(int(each[2]))}
        ordersList.append(order)
    return ordersList


def get_order(id):
    curr = conn.cursor()
    curr.execute('select customerId, productId, date from orders where orders.id=(%s)', (id,))
    one = curr.fetchone()
    order = {'id': one[0], 'customerId': one[1], 'productId': one[2], 'date': one[3]}
    return order


def upsert_order(order):
    curr = conn.cursor()
    # assuming no conflicts because a customer can order multiple of the same items on the same day
    curr.execute('insert into orders (customerId, productId, date) values (%s, %s, %s)', (order['customerId'], order['productId'], order['date']))
    conn.commit()

def delete_order(id):
    curr = conn.cursor()
    curr.execute('delete from orders where orders.id=(%s)', (id,))
    conn.commit()

# Return a list of products.  For each product, build
# create and populate a last_order_date, total_sales, and
# gross_revenue property.  Use JOIN and aggregation to avoid
# accessing the database more than once, and retrieving unnecessary
# information
def sales_report():
    curr = conn.cursor()
    curr.execute("select products.id, count(products.id), price, max(date) from products join orders on products.id = orders.productId GROUP BY products.id")
    salesReport = []
    for product in curr:
        revenue = product[1] * product[2]
        productReport = {'id': product[0], 'total_sales': product[1], 'gross_revenue': revenue, 'last_order_date': product[3]}
        salesReport.append(productReport)
    return salesReport




