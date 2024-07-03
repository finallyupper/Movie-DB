from mysql.connector import connect, Error
import pandas as pd 
from utils import *
import sys  
import time
from threading import Thread 

MIN_AGE = 12
MAX_AGE = 110
MIN_PRICE = 0
MAX_PRICE = 100000
MIN_RATINGS = 1
MAX_RATINGS = 5 

########## ENTER YOUR DATABASE NAME BELOW ###########
db_name = "movie_db"
#####################################################

def get_database_connection(database=None):
    """
    Establishes a connection to the MySQL server and returns the connection object.
    If a database name is provided, it connects to that database.
    """
    try:
        connection = connect(
            host='127.0.0.1',
            user='Enter your user name',
            password='Enter your password',
            database=database,
            charset='utf8'
        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        exit()

# 1. Initialize database
def init_db(data: pd.DataFrame)->None:
    """
    Initializes the database by creating necessary tables and inserting initial data.
    :param data: DataFrame containing the initial data to populate the database.
    """

    create_customer_table = """ 
        CREATE TABLE IF NOT EXISTS customer (
            cus_id int AUTO_INCREMENT PRIMARY KEY,
            cus_name varchar(255) NOT NULL,
            cus_age int CHECK (cus_age BETWEEN 12 AND 111),
            UNIQUE (cus_name, cus_age)
        ) AUTO_INCREMENT=1;
    """
    
    create_director_table = """
        CREATE TABLE IF NOT EXISTS director (
            dir_id int AUTO_INCREMENT PRIMARY KEY,
            dir_name varchar(255) NOT NULL,
            UNIQUE (dir_name)
        ) AUTO_INCREMENT=1;
    """
    
    create_movie_table = """
        CREATE TABLE IF NOT EXISTS movie (
            mov_id int AUTO_INCREMENT PRIMARY KEY,
            mov_title varchar(255) NOT NULL,
            mov_price int CHECK (mov_price BETWEEN 0 AND 100001),
            UNIQUE (mov_title)
        ) AUTO_INCREMENT=1;
    """
    
    create_movie_direction_table = """
        CREATE TABLE IF NOT EXISTS movie_direction (
            dir_id int,
            mov_id int,
            FOREIGN KEY (dir_id) REFERENCES director(dir_id) ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (mov_id) REFERENCES movie(mov_id) ON UPDATE CASCADE ON DELETE CASCADE,
            PRIMARY KEY (dir_id, mov_id)
        );
    """
    
    create_rating_table = """
        CREATE TABLE IF NOT EXISTS rating (
            mov_id int,
            cus_id int,
            cus_stars int CHECK (cus_stars BETWEEN 1 AND 6) DEFAULT NULL,
            FOREIGN KEY (mov_id) REFERENCES movie(mov_id) ON DELETE CASCADE,
            FOREIGN KEY (cus_id) REFERENCES customer(cus_id) ON DELETE CASCADE
        );
    """
    
    create_booking_table = """
        CREATE TABLE IF NOT EXISTS booking (
            booking_id int AUTO_INCREMENT PRIMARY KEY,
            mov_id int,
            cus_id int,
            booking_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
            FOREIGN KEY (mov_id) REFERENCES movie(mov_id) ON DELETE CASCADE,
            FOREIGN KEY (cus_id) REFERENCES customer(cus_id) ON DELETE CASCADE,
            UNIQUE (mov_id, cus_id)
        ) AUTO_INCREMENT=1;
    """
    global db_name 
    connection = get_database_connection(db_name) 
    create_table(connection, create_customer_table)
    create_table(connection, create_director_table)
    create_table(connection, create_movie_table)
    create_table(connection, create_movie_direction_table)
    create_table(connection, create_rating_table)
    create_table(connection, create_booking_table)
    
    
    connection = get_database_connection(db_name)
    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
                for _, row in data.iterrows():
                    # 1. insert data into director table
                    cursor.execute('SELECT COUNT(*) FROM director WHERE dir_name = %s;', (row['director'],))
                    res = cursor.fetchone()
                    if res['COUNT(*)'] == 0:
                        cursor.execute('INSERT INTO director(dir_name) VALUES (%s);', (row['director'],))

                    # 2. insert data into movie table
                    cursor.execute('SELECT COUNT(*) FROM movie WHERE mov_title = %s;', (row['title'],))
                    res = cursor.fetchone()
                    if res['COUNT(*)'] == 0:
                        cursor.execute('INSERT INTO movie(mov_title, mov_price) VALUES (%s, %s);', (row['title'], row['price']))

                    # 3. insert data into customer table
                    cursor.execute('SELECT COUNT(*) FROM customer WHERE cus_name = %s AND cus_age = %s;', (row['name'], row['age']))
                    res = cursor.fetchone()
                    if res['COUNT(*)'] == 0:
                        cursor.execute('INSERT INTO customer(cus_name, cus_age) VALUES (%s, %s);', (row['name'], row['age']))

                    # 4.movie_direction
                    cursor.execute('SELECT dir_id FROM director WHERE dir_name = %s;', (row['director'],))
                    dir_id = cursor.fetchall()[0]['dir_id'] # [{'dir_id': 60}]
                    cursor.execute('SELECT mov_id FROM movie WHERE mov_title = %s;', (row['title'],))
                    mov_id = cursor.fetchall()[0]['mov_id'] # [{'dir_id': 60}]
                    cursor.execute('SELECT COUNT(*) as count FROM movie_direction WHERE dir_id = %s AND mov_id = %s;', (dir_id, mov_id))
                    res = cursor.fetchone()
                    if res['count'] == 0:
                        cursor.execute('INSERT INTO movie_direction(dir_id, mov_id) VALUES (%s, %s);', (dir_id, mov_id))

                    # 6. booking
                    cursor.execute('SELECT cus_id FROM customer WHERE cus_name = %s AND cus_age = %s;', (row['name'], row['age']))                    
                    cus_id = cursor.fetchall()[0]['cus_id']
                    cursor.execute('SELECT mov_id FROM movie WHERE mov_title = %s;', (row['title'],))
                    mov_id = cursor.fetchall()[0]['mov_id'] 
                    cursor.execute('SELECT COUNT(*) as count FROM booking WHERE cus_id = %s AND mov_id = %s;', (cus_id, mov_id))
                    res = cursor.fetchone()
                    if res['count'] == 0:
                        cursor.execute('INSERT INTO booking(cus_id, mov_id) VALUES (%s, %s);', (cus_id, mov_id))

                    # 5. rating
                    cursor.execute('SELECT cus_id FROM customer WHERE cus_name = %s AND cus_age = %s;', (row['name'], row['age']))
                    cus_id = cursor.fetchall()[0]['cus_id'] # [{'dir_id': 60}]
                    cursor.execute('SELECT mov_id FROM movie WHERE mov_title = %s;', (row['title'],))
                    mov_id = cursor.fetchall()[0]['mov_id'] # [{'dir_id': 60}]
                    cursor.execute('SELECT COUNT(*) as count FROM rating WHERE cus_id = %s AND mov_id = %s;', (cus_id, mov_id))
                    res = cursor.fetchone()
                    if res['count'] == 0:
                        cursor.execute('INSERT INTO rating(cus_id, mov_id) VALUES (%s, %s);', (cus_id, mov_id))
                    


            connection.commit()
            print('Database successfully initialized')
        except Error as e:
            print(f"Error: {e}")
        finally:
            connection.close()

# 2. Print all movies
def show_movies()->None: 
    """
    Print all movies (info).
    - id, title, director, price, reservation, avg. rating
    """
    script = """ 
        SELECT 
            movie.mov_id AS id, 
            movie.mov_title AS title, 
            director.dir_name AS director,
            movie.mov_price AS price, 
            COUNT(booking.booking_id) AS reservation, 
            AVG(rating.cus_stars) AS `avg. rating`
        FROM 
            movie
        JOIN 
            movie_direction ON movie.mov_id = movie_direction.mov_id
        JOIN 
            director ON movie_direction.dir_id = director.dir_id
        LEFT OUTER JOIN 
            rating ON movie.mov_id = rating.mov_id
        LEFT OUTER JOIN
            booking ON movie.mov_id = booking.mov_id AND rating.cus_id = booking.cus_id
        GROUP BY 
            movie.mov_id, movie.mov_title, director.dir_name, movie.mov_price
        ORDER BY 
            movie.mov_id;
        """ 
    global db_name 
    connection = get_database_connection(db_name)
    if connection:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(script)
            result = cursor.fetchall()
            print("-" * 80)
            print("{:<5} {:<70} {:<33} {:<10} {:<15} {:<15}".format("id", "title", "director", "price", "reservation", "avg. rating"))
            print("-" * 80)
            for row in result:
                id, title, director, price, reservation, avg_rating = [x if x is not None else 'None' for x in row.values()]
                print("{:<5} {:<70} {:<33} {:<10} {:<15} {:<15}".format(id, title, director, price, reservation, avg_rating))
            print("-" * 80)
        connection.close()
    
# 3. Print all users
def show_users()->None:
    """
    Print all users.  
    - id, name, age
    """
    global db_name 
    connection = get_database_connection(db_name)
    if connection:
        with connection.cursor(dictionary=True) as cursor:
            script = """
                SELECT cus_id as id, cus_name as name, cus_age as age 
                FROM customer
                ORDER BY cus_id;
            """
            cursor.execute(script) 
            result = cursor.fetchall()
            print("-" * 80)
            print("{:<8} {:<50} {:<15}".format("id", "name", "age"))
            print("-" * 80)
            for row in result:
                id, name, age = row.values()
                print("{:<8} {:<50} {:<15}".format(id, name, age))
            print("-" * 80)
        connection.close()

# 4. Insert a new movie
def insert_movie(movie_title:str, movie_director:str, movie_price:int):
    """
    Insert a new movie into the database.
    :param movie_title: Title of the movie.
    :param movie_director: Name of the movie director.
    :param movie_price: Price of the movie.
    """ 
    global db_name 
    if movie_price < MIN_PRICE  or movie_price > MAX_PRICE :
        print(f'Movie price should be from {MIN_PRICE} to {MAX_PRICE}')
        return
    connection = get_database_connection(db_name)
    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
                sql_movie = "INSERT INTO movie(mov_title, mov_price) VALUES (%s, %s);"
                cursor.execute(sql_movie, (movie_title, movie_price))
                sql_director = "INSERT INTO director(dir_name) VALUES (%s);"
                cursor.execute(sql_director, (movie_director,))
                
                # Also update movie_direction table
                cursor.execute("SELECT * FROM movie WHERE mov_title = %s;", (movie_title,))
                movie_id = cursor.fetchone()['mov_id']
                cursor.execute("SELECT * FROM director WHERE dir_name = %s;", (movie_director,))
                director_id = cursor.fetchone()['dir_id']
                cursor.execute("INSERT INTO movie_direction (dir_id, mov_id) VALUES (%s, %s);", (director_id, movie_id))
            
            connection.commit()
            print('One movie successfully inserted')
        except Error:
            print(f"Movie {movie_title} already exists")     
            return 
        finally:
            connection.close()

# 5. Remove a movie
def insert_customer(customer_name:str, customer_age:int):
    """
    Insert new customer into the database.
    :param customer_name: Name of the customer.
    :param customer_age: Age of the customer.
    """
    if customer_age < MIN_AGE or customer_age > MAX_AGE:
        print(f'User age should be from {MIN_AGE} to {MAX_AGE}')
        return 
    global db_name 
    connection = get_database_connection(db_name)
    if connection:
        try:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute("INSERT INTO customer(cus_name, cus_age) VALUES (%s, %s);", (customer_name, customer_age))
            connection.commit()
        except Error:
            print(f'User {(customer_name, customer_age)} already exists')
            return 
        finally:
            connection.close()
    print('One user successfully inserted')

# 6. Insert a new user
def delete_movie(movie_id:int):
    """
    Delete a movie.
    :param movie_id: Id of the movie.
    * Delete corresponding booking information and ratings
    """
    # Check the existance of the movie
    if not check_movie(movie_id):
        print(f'Movie {movie_id} does not exist')
        return 
    global db_name 
    connection = get_database_connection(db_name)
    with connection.cursor(dictionary=True) as cursor:
            cursor.execute("DELETE from movie WHERE mov_id = %s;", (movie_id, ))
            result = cursor.fetchall()    
            connection.commit()
    connection.close()
    print('One movie successfully removed')

# 7. Remove a user
def delete_user(customer_id:int):
    """
    Delete a customer
    :param customer_id: Id of the customer.
    * Delete corresponding booking information and ratings
    """
    # Check the existance of the user
    if not check_customer(customer_id):
        print(f'User {customer_id} does not exist')
        return 
    global db_name 
    connection = get_database_connection(db_name)
    with connection.cursor(dictionary=True) as cursor:
            cursor.execute("DELETE from customer WHERE cus_id = %s;", (customer_id, ))
            result = cursor.fetchall()    
            connection.commit()
    connection.close()
    print('One user successfully removed')

# 8. Book a movie
def book_movie(movie_id: int, customer_id: int):
    """
    Book a movie
    :param movie_id: Id of the movie.
    :param customer_id: Id of the customer(user).
    """
    global db_name 
    connection = get_database_connection(db_name)

    try:
        # 3. Check the existance of the movie
        if not check_movie(movie_id):
            print(f'Movie {movie_id} does not exist')
            return
        # 4. Check the existance of the user
        if not check_customer(customer_id):
            print(f'User {customer_id} does not exist')
            return
    
        with connection.cursor(dictionary=True) as cursor:
            # 1. Check if user has already booked the movie
            if check_reserved(movie_id, customer_id):
                print(f'User {customer_id} has already booked movie {movie_id}')
                return
            
            # 2. Check if the movie is fully booked (up to 10 people)
            if not check_fully_booked(movie_id):
                print(f'Movie {movie_id} has already been fully booked')
                return
            
            # 5. Book the movie
            cursor.execute("INSERT INTO booking (mov_id, cus_id) VALUES (%s, %s);", (movie_id, customer_id))
            connection.commit()
            print('Movie successfully booked')

            # 6. Reflect to rating table
            cursor.execute("INSERT INTO rating(mov_id, cus_id) VALUES (%s, %s);", (movie_id, customer_id))
            connection.commit()

    except Error as e:
        print(e)

    finally:
        connection.close()

# 9. Rate a movie
def rate_movie(movie_id:int, customer_id:int, ratings:int):
    """
    Rate a movie.
    :param movie_id: id of the movie.
    :param customer_id: id of the customer.
    :param ratings: ratings for the movie. (1~5)
    """
    # 1. check the existance of movie id
    if not check_movie(movie_id):
        print(f'Movie {movie_id} does not exist')
        return
    # 2. check the existance of customer id
    if not check_customer(customer_id):
        print(f'User {customer_id} does not exist')
        return
    global db_name 
    connection = get_database_connection(db_name)
    with connection.cursor(dictionary=True) as cursor:
        # 3. check the reservation of the movie
        if not check_reserved(movie_id, customer_id):
            print(f'User {customer_id} has not booked movie {movie_id} yet')
            return
        
        # 4. check if the user already rated    
        if check_rated(movie_id, customer_id):
            print(f'User {customer_id} has already rated movie {movie_id}')
            return
        
    # 5. check the range of ratings
    if ratings < MIN_RATINGS or ratings > MAX_RATINGS:
        print('Ratings should be from 1 to 5')
        return
    # End of checks

    
    # 6. Insert the new rating
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("""UPDATE  rating
                          SET cus_stars = %s
                          WHERE mov_id = %s and cus_id = %s;""", ((ratings, movie_id, customer_id)))
        cursor.execute("""
            SELECT * FROM rating;
        """)
        result = cursor.fetchall()
        connection.commit()
    print("Movie successfully rated")
    connection.close()

# 10. Print all users who booked for a movie
def show_booked_users(movie_id:int):
    """
    Print all users who booked for a movie.
    :param movie_id: id of the movie.
    """
    # Check the existance of the movie
    if not check_movie(movie_id):
        print(f'Movie {movie_id} does not exist')
        return
    global db_name 
    connection = get_database_connection(db_name)
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(""" 
            SELECT cus_id as id, cus_name as name, cus_age as age, IFNULL(rating.cus_stars, 'None') as rating
            FROM booking 
                    NATURAL JOIN customer 
                    NATURAL LEFT OUTER JOIN rating
            WHERE mov_id = %s
            ORDER BY cus_id;
            """, (movie_id, ))
        result = cursor.fetchall()
        print("-" * 80)
        print("{:<5} {:<50} {:<10} {:<15}".format("id", "name", "age", "rating"))
        print("-" * 80)
        for row in result:
            id, name, age, rating = row.values()
            print("{:<5} {:<50} {:<10} {:<15}".format(id, name, age, rating))
        print("-" * 80)
    connection.close()

# 11. Print all movies boooked by a user
def show_booked_movies(customer_id:int):
    """
    Print all movies booked by a user.
    :param customer_id: id of the customer.
    """
    # Check the existance of the user
    if not check_customer(customer_id):
        print(f'User {customer_id} does not exist')
        return
    global db_name 
    connection = get_database_connection(db_name)
    with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM customer WHERE cus_id = %s;", (customer_id, )) 
            result = cursor.fetchall()    
            if len(result) <= 0:
                print(f'User {customer_id} does not exist')
                return

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(""" 
                SELECT 
                    movie.mov_id AS id, 
                    movie.mov_title AS title, 
                    director.dir_name AS director, 
                    movie.mov_price AS price, 
                    IFNULL(rating.cus_stars, 'None') AS rating
                FROM 
                    booking
                    JOIN movie ON booking.mov_id = movie.mov_id
                    JOIN movie_direction ON movie.mov_id = movie_direction.mov_id
                    JOIN director ON movie_direction.dir_id = director.dir_id
                    LEFT OUTER JOIN rating ON movie.mov_id = rating.mov_id AND booking.cus_id = rating.cus_id
                WHERE 
                    booking.cus_id = %s
                ORDER BY 
                    movie.mov_id;
            """, (customer_id, ))
        result = cursor.fetchall()
        print("-" * 80)
        print("{:<5} {:<70} {:<33} {:<10} {:<15}".format("id", "title", "director", "price", "rating"))
        print("-" * 80)
        for row in result:
            id, title, director, price, rating = row.values()
            print("{:<5} {:<70} {:<33} {:<10} {:<15}".format(id, title, director, price, rating))
        print("-" * 80)
    connection.close()    

# 12. Reset database
def reset_db(movie_data: pd.DataFrame):
    """
    Reset the database by dropping existing tables
    :param movie_data: DataFrame containing the initial data to populate the database.
    """
    global db_name 
    connection = get_database_connection(db_name)
    try:
        with connection.cursor(dictionary=True) as cursor:
            # Disable foreign key checks and drop tables
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute("DROP TABLE IF EXISTS movie, director, movie_direction, customer, rating, booking;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            connection.commit() 
        print('All tables successfully dropped')
        
    except Error as e:
        print(f"Error dropping tables: {e}")
        return
    finally:
        connection.close()

    def init_db_with_loading():
        """
        Function to initialize the database in a separate thread
        """
        init_db(movie_data)
        print("Database initialization complete.")

    # Start the database initialization in a separate thread
    init_thread = Thread(target=init_db_with_loading)
    init_thread.start()

    print("Initializing the database, please wait...")
    while init_thread.is_alive():
        for i in range(3):
            print(f"Loading{'.' * (i + 1)}", end='\r')
            time.sleep(1)
    init_thread.join()

def main():
    connection = get_database_connection()
    
    global db_name 

    drop_database(connection, db_name) # Drop the database if it exists
    create_database(connection, db_name) # Create a new database

    start = 1
    action = -1

    movie_data = pd.read_csv('./data/data.csv') # Refactored on 2024/07

    connection.close()
    connection = get_database_connection(db_name)

    while action != 12:
        show_menu()

        action = get_user_input('Select your action: ', int)

        if start:
            if action != 12 and action != 1:
                print('You must first initialize the database')
                print()
                continue

        if action == 1:
            if start:
                init_db(movie_data)
                start = 0
                print() 
            else:
                print('If you want to re-initialize the database, please use action 13.')
                print()

        elif action == 2:
            show_movies()
            print()

        elif action == 3:
            show_users()
            print()
            
        elif action == 4:
            movie_title = input('Movie title: ')
            movie_director = input('Movie director: ')
            movie_price = int(input('Movie price: '))
            insert_movie(movie_title, movie_director, movie_price)
            print()

        elif action == 5:
            movie_id = int(input("Movie id: "))
            delete_movie(movie_id)
            print()

        elif action == 6:
            customer_name = input('User name: ')
            customer_age = int(input('User age: '))
            insert_customer(customer_name, customer_age)
            print()

        elif action == 7:
            customer_id = int(input("User ID: "))
            delete_user(customer_id)
            print()

        elif action == 8:
            movie_id = int(input('Movie ID: '))
            customer_id = int(input('User ID: '))
            book_movie(movie_id, customer_id)
            print()

        elif action == 9:
            movie_id = int(input('Movie ID: '))
            customer_id = int(input('User ID: '))
            ratings = int(input('Ratings (1~5): '))
            rate_movie(movie_id, customer_id, ratings)
            print()

        elif action == 10:
            movie_id = int(input('Movie ID: '))
            show_booked_users(movie_id)
            print()

        elif action == 11:
            customer_id = int(input('User ID: '))
            show_booked_movies(customer_id)
            print()

        elif action == 12:
            print('Bye!')
            
        elif action == 13:
            check = input('Do you really want to reset the database? (y/n) : ')
            if check not in ['y', 'n']:
                print('Only y/n answer is available!')
            elif check == 'y':
                reset_db(movie_data) 
            print()       

        else:
            print('Invalid action')
            print()

if __name__ == "__main__":
    main()