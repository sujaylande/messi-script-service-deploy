import mysql.connector
import os
import numpy as np
from dotenv import load_dotenv
load_dotenv()

# def get_db_connection(): 
#     return mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME"),
#         ssl_ca=os.getenv("DB_SSL_CA")  # correct usage
#     )

# Create pool only once at startup
pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    pool_reset_session=True,
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    ssl_ca=os.getenv("DB_SSL_CA")
)

def get_db_connection():
    return pool.get_connection()


def insert_student(conn, student_data):
    print("insert st", student_data)
    cursor = conn.cursor()
    query = """
        INSERT INTO students (name, email, reg_no, roll_no, block_no)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        student_data["name"], student_data["email"], student_data["reg_no"],
        student_data["roll_no"], student_data["block_no"]
    ))
    conn.commit()
    cursor.close()


def insert_embedding(conn, reg_no, block_no, embedding):
    print("inset em", reg_no, block_no)
    cursor = conn.cursor()
    query = "INSERT INTO face_embeddings (reg_no, block_no, embedding) VALUES (%s, %s, %s)"
    cursor.execute(query, (reg_no, block_no, embedding.tobytes()))
    conn.commit()
    cursor.close()


def get_all_embeddings(conn, block_no):
    cursor = conn.cursor()
    cursor.execute("SELECT reg_no, block_no, embedding FROM face_embeddings WHERE block_no = %s", (block_no,))
    results = cursor.fetchall()
    embeddings = [(reg_no, np.frombuffer(embedding_blob, dtype=np.float64)) for reg_no, _, embedding_blob in results]
    cursor.close()
    return embeddings



def insert_attendance(conn, reg_no, block_no, meal_slot, meal_cost, timestamp, date):
    print("insert_attendance", meal_slot)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO attendance (reg_no, block_no, meal_slot, meal_cost, timestamp, date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (reg_no, block_no, meal_slot, meal_cost, timestamp, date))
    conn.commit()
    cursor.close()


def has_eaten(conn, reg_no, block_no, meal_slot, date):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM attendance
        WHERE reg_no = %s AND block_no = %s AND meal_slot = %s AND date = %s
    """, (reg_no, block_no, meal_slot, date))
    result = cursor.fetchone()
    cursor.close()
    return result is not None


def get_student_info(conn, reg_no, block_no):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM students WHERE reg_no = %s AND block_no = %s", (reg_no, block_no))
    student = cursor.fetchone()
    cursor.close()
    return student

def delete_student(conn, reg_no, block_no):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("DELETE FROM students WHERE reg_no = %s AND block_no = %s", (reg_no, block_no))
    student = cursor.fetchone()
    cursor.close()
    return student