import psycopg2
import os
from dotenv import load_dotenv
import random
from faker import Faker
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz

local_tz = pytz.timezone('Asia/Jakarta')
fake = Faker()
load_dotenv()

dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

def create_connection():
    """Create connection to database PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connection to database successful!")
        return conn
    except Exception as e:
        print(f"Error when connection to database successful!: {e}")
        return None

def create_schema_and_tables():
    """Create the required schema and tables in PostgreSQL."""
    with create_connection() as conn:
        if conn is None:
            return

        with conn.cursor() as cursor:
            try:
                # create schema if not exists
                cursor.execute("CREATE SCHEMA IF NOT EXISTS finpro;")
                
                # create users table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS finpro.users (
                    user_id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    address TEXT,
                    gender VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                # create books table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS finpro.books (
                    book_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    author VARCHAR(100) NOT NULL,
                    book_type VARCHAR(100),
                    genre VARCHAR(100),
                    publisher VARCHAR(100),
                    release_year INT,
                    stock INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                # create rents table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS finpro.rents (
                    rent_id SERIAL PRIMARY KEY,
                    user_id INT REFERENCES finpro.users(user_id) NOT NULL,
                    book_id INT REFERENCES finpro.books(book_id) NOT NULL,
                    rent_date TIMESTAMP,
                    return_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                # Commit change
                conn.commit()
                print("schema and tables created successfully")
                
            except Exception as e:
                print(f"Error when creating schema or table: {e}")
                conn.rollback()

def generate_data():
    # Generate users
    users = []
    for _ in range(10):
        name = fake.name()
        email = name.lower().replace(" ", ".") + "@example.com"
        address = fake.address().replace("\n", ", ")
        gender = random.choice(['Male', 'Female'])
        created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
        users.append((name, email, address, gender, created_at))
    
    # Generate books
    books = []
    for _ in range(10):
        title = fake.sentence(nb_words=3)
        author = fake.name()
        book_type = random.choice(['Novel','Comic','Biography','Encyclopedia','Magazine'])
        genre = random.choice(['Fiction','Non-Fiction','Mystery','Thriller','Romance','Technology'])
        publisher = fake.company()
        release_year = random.randint(2000, 2023)
        stock = max(1,random.randint(1, 10))  # Ensure stock is at least 1 for availability
        created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
        books.append((title, author, book_type, genre, publisher, release_year, stock, created_at))
    
    return users, books


def insert_data(users, books):
    conn = create_connection()
    if conn is None:
        return

    cursor = conn.cursor()

    create_schema_and_tables()
    
    try:
        # Logging for checking data
        print(f"Users: {users}")
        print(f"Books: {books}")
        # Insert users
        cursor.executemany("INSERT INTO finpro.users (name, email, address, gender, created_at) VALUES (%s, %s, %s, %s, %s)", users)

        # Insert books
        cursor.executemany("INSERT INTO finpro.books (title, author, book_type, genre, publisher, release_year, stock, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", books)

        # Insert rents
        cursor.execute("SELECT user_id FROM finpro.users")
        user_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT book_id, stock FROM finpro.books WHERE stock > 0")
        available_books = cursor.fetchall()

        if not user_ids:
            print("No users found in the database. Skipping rents insertion.")
            return

        if not available_books:
            print("No available books with stock > 0. Skipping rents insertion.")
            return

        rents = []
        for _ in range(10):
            user_id = random.choice(user_ids)
            book_id, stock = random.choice(available_books)
            rent_date = (datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(days=random.randint(3, 7))).strftime('%Y-%m-%d %H:%M:%S')
            return_date = (datetime.strptime(rent_date, '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 3))).strftime('%Y-%m-%d %H:%M:%S')
            created_at = created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
            rents.append((user_id, book_id, rent_date, return_date, created_at))

            # Update book stock
            cursor.execute("UPDATE finpro.books SET stock = stock - 1 WHERE book_id = %s", (book_id,))

        cursor.executemany("INSERT INTO finpro.rents (user_id, book_id, rent_date, return_date, created_at) VALUES (%s, %s, %s, %s, %s)", rents)

        # Commit transaction
        conn.commit()
        print("Data successfully inserted into table")

    except Exception as e:
        print(f"Error while inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    users, books = generate_data()
    insert_data(users, books)
