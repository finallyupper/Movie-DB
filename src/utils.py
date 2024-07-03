from mysql.connector import connect, Error
from run import get_database_connection, db_name 
import sys 

def get_user_input(prompt, input_type=str):
    while True:
        try:
            return input_type(input(prompt))
        except ValueError:
            print(f"Invalid input. Please enter a valid {input_type.__name__}.")

def show_databases(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")
            databases = cursor.fetchall()
            return [db[0] for db in databases]
    except Error as e:
        print(f"Error: {e}")
        exit()

def show_ratings(table_name:str)->None:
    """
    Used for debugging
    :param table_name: Name of the table.
    """
    global db_name
    connection = get_database_connection(db_name)
    if connection:
        with connection.cursor(dictionary=True) as cursor:
            script = """
                SELECT *
                FROM %s;
            """, ((table_name, ))
            cursor.execute(script) 
            results = cursor.fetchall()
            for r in results:
                print(r)

def create_table(connection, create_table_sql):
    """
    Creates a table based on the provided SQL script.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
        exit() 
        
def check_movie(movie_id:int)->int:
    """
    Check the existance of the movie.
    :param movie_id: id of the movie.
    returns 1 if ihe movie exists else 0
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM movie WHERE mov_id = %s;", (movie_id, )) 
            movie = cursor.fetchall()    
            if len(movie) <= 0:
                return 0
    except Error as e:
        print(e)
        sys.exit()
    return 1 

def check_customer(customer_id:int)->int:
    """ 
    Check the existance of the user.
    :param customer_id: id of the customer.
    returns 1 if ihe user exists else 0
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM customer WHERE cus_id = %s;", (customer_id, )) 
            customer = cursor.fetchall()  
            if len(customer) <= 0:
                return 0  
    except Error as e:
        print(e)
        sys.exit()
    return 1 

def check_fully_booked(movie_id:int)->int:
    """
    Check if the movie is fully booked (up to 10 people).
    :param movie_id: id of the movie.
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT COUNT(booking_id) AS booking_count FROM booking WHERE mov_id = %s;", (movie_id,))
            n_books = cursor.fetchall()[0]['booking_count']
            if n_books >= 10:
                return 0
    except Error as e:
        print(e)
        sys.exit()
    return 1 

def check_reserved(movie_id:int, customer_id:int)->int:
    """
    check the reservation of the movie.
    :param movie_id: id of the movie.
    :param customer_id: id of the customer.
    returns 1 if reserved
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT booking_id FROM booking WHERE cus_id = %s AND mov_id = %s;", (customer_id, movie_id))
            if len(cursor.fetchall()) <= 0:      
                return 0
    except Error as e:
        print(e)
        sys.exit()
    return 1 

def check_rated(movie_id:int, customer_id:int)->int:
    """
    check if the user already rated.
    :param movie_id: id of the movie.
    :param customer_id: id of the customer.
    returns 1 if rated
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT cus_stars FROM rating WHERE cus_id = %s AND mov_id = %s;", (customer_id, movie_id))
            result = cursor.fetchone()
            if result and result['cus_stars'] is not None:
                return 1 
    except Error as e:
        print(e)
        sys.exit()
    return 0

def create_database(connection, db_name):
    """
    Creates a new database with the given name.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database {db_name} created successfully.")
    except Error as e:
        print(f"Error creating database {db_name}: {e}")
        exit()

def drop_database(connection, db_name):
    """
    Drops the database with the given name if it exists
    """
    try:
        with connection.cursor() as cursor:
            # Check if the database exists
            cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
            result = cursor.fetchone()
            
            if result:
                # Drop the database if it exists
                cursor.execute(f"DROP DATABASE {db_name}")
                print(f"Database {db_name} dropped successfully.")
                
    except Error as e:
        print(f"Error dropping database {db_name}: {e}")
        exit()

def show_menu()->None:
    print('============================================================')
    print('1. initialize database')
    print('2. print all movies')
    print('3. print all users')
    print('4. insert a new movie')
    print('5. remove a movie')
    print('6. insert a new user')
    print('7. remove a user')
    print('8. book a movie')
    print('9. rate a movie')
    print('10. print all users who booked for a movie')
    print('11. print all movies booked by a user')
    print('12. exit')
    print('13. reset database')
    print('============================================================')
