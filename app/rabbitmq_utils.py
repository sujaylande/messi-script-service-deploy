import pika
import json
import os
from dotenv import load_dotenv
load_dotenv()
from app.db_utils import get_db_connection
import threading
import subprocess
import concurrent.futures
from app.db_utils import insert_student, insert_embedding, delete_student
from app.face_utils import capture_face
import logging
from app.models import RegisterStudent, DeleteStudentRequest
from app.db_utils import insert_attendance


logger = logging.getLogger(__name__)


amqp_url = os.getenv("AMQP_URL")
params = pika.URLParameters(amqp_url)
params.heartbeat = 600
params.blocked_connection_timeout = 300

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='register_student_queue_for_manager', durable=True)
channel.queue_declare(queue='attendance_queue_for_student', durable=True)
channel.queue_declare(queue='register_student_queue_for_student', durable=True)
channel.queue_declare(queue='attendance_queue_for_manager', durable=True)
channel.queue_declare(queue='register_status_queue_for_manager', durable=True)


def send_registration_event_toManager(payload):
    payload["secret"] = SHARED_SECRET
    channel.basic_publish(
        exchange='',
        routing_key='register_student_queue_for_manager',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )

def send_registration_event_toStudent(payload):
    payload["secret"] = SHARED_SECRET
    channel.basic_publish(
        exchange='',
        routing_key='register_student_queue_for_student',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )

SHARED_SECRET = os.getenv("SHARED_SECRET")

def send_attendance_event_toStudent(payload):
    payload["secret"] = SHARED_SECRET
    channel.basic_publish(
        exchange='',
        routing_key='attendance_queue_for_student',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )

def send_attendance_event_toManager(payload):
    print("sending att to manager")
    payload["secret"] = SHARED_SECRET
    channel.basic_publish(
        exchange='',
        routing_key='attendance_queue_for_manager',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )

def send_registration_Status_toManager(payload):
    payload["secret"] = SHARED_SECRET
    channel.basic_publish(
        exchange='',
        routing_key='register_status_queue_for_manager',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )


def start_feedback_consumer():
    def feedback_callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            print("📥 Received feedback in script service:", data)

            # 🛡️ Secret verification
            secret = data.pop("secret", None)
            if secret != SHARED_SECRET:
                print("❌ Unauthorized feedback message. Ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            conn = get_db_connection()
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO feedback (
                    reg_no, block_no, meal_type,
                    taste_rating, hygiene_rating, quantity_rating,
                    want_change, comments, feedback_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURDATE())
            """

            cursor.execute(insert_query, (
                data['reg_no'],
                data['block_no'],
                data['meal_type'],
                data['taste'],
                data['hygiene'],
                data['quantity'],
                data.get('want_change', ''),
                data.get('comments', '')
            ))

            conn.commit()
            cursor.close()
            conn.close()

            print("✅ Stored feedback to script DB.")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print("❌ Error processing feedback:", str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run():
        try:
            # 👇 Create a NEW connection and channel for this thread
            thread_connection = pika.BlockingConnection(params)
            thread_channel = thread_connection.channel()
            thread_channel.queue_declare(queue='feedback_queue_for_script_service', durable=True)
            thread_channel.basic_consume(
                queue='feedback_queue_for_script_service',
                on_message_callback=feedback_callback
            )
            print("🚀 Feedback consumer started")
            thread_channel.start_consuming()
        except Exception as e:
            print("❌ Consumer thread error:", e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()


def start_attendance_consumer():
    def attendance_callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            print("📥 Received attendance request:", data)

            block_no = data.get("block_no")
            secret = data.get("secret")
            expected_secret = os.getenv("SHARED_SECRET")

            if not block_no or secret != expected_secret:
                print("❌ Invalid or unauthorized attendance request.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            subprocess.Popen(["python", "-m", "app.attendance_worker", str(block_no)])
            print("✅ Launched attendance worker for block:", block_no)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print("❌ Error in attendance callback:", str(e))

    def run():
        try:
            # 👇 Create a NEW connection and channel for this thread
            thread_connection = pika.BlockingConnection(params)
            thread_channel = thread_connection.channel()
            thread_channel.queue_declare(queue='start_attendance_queue_for_script', durable=True)
            thread_channel.basic_consume(
                queue='start_attendance_queue_for_script',
                on_message_callback=attendance_callback
            )
            print("🚀 Attendance consumer started")
            thread_channel.start_consuming()
        except Exception as e:
            print("❌ Consumer thread error:", e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()


executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def process_registration(data: RegisterStudent):
    try:
        embedding = capture_face()
        conn = get_db_connection()
        insert_student(conn, data.dict())
        insert_embedding(conn, data.reg_no, data.block_no, embedding)
        send_registration_event_toManager(data.dict())
        send_registration_event_toStudent(data.dict())
        send_registration_Status_toManager({
            "reg_no": data.reg_no,
            "status": "success"
        })
        conn.close()
    except Exception as e:
        # On failure
        send_registration_Status_toManager({
            "reg_no": data.reg_no,
            "status": "fail"
        })
        logger.error(f"Error in background registration task: {e}", exc_info=True)


def start_registration_consumer():
    def registration_callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            print("📥 Received registration data:", data)

            secret = data.pop("secret", None)
            expected_secret = os.getenv("SHARED_SECRET")

            if secret != expected_secret:
                print("❌ Unauthorized registration attempt.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            payload = data.get("payload")
            if not payload:
                print("❌ Payload missing in registration data.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            from app.models import RegisterStudent
            student_data = RegisterStudent(**payload)

            print("✅ Parsed student data:", student_data)

            # 🚀 Offload to background thread
            def run_registration():
                try:
                    process_registration(student_data)
                except Exception as e:
                    print("❌ Error in registration task:", e)

            executor.submit(run_registration)

            # ✅ Acknowledge immediately
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print("❌ Callback level error:", str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Prevent requeue


    def run():
        try:
            # 👇 Create a NEW connection and channel for this thread
            thread_connection = pika.BlockingConnection(params)
            thread_channel = thread_connection.channel()
            thread_channel.queue_declare(queue='start_registration_queue_for_script', durable=True)
            thread_channel.basic_consume(
                queue='start_registration_queue_for_script',
                on_message_callback=registration_callback
            )
            print("🚀 Registration consumer started")
            thread_channel.start_consuming()
        except Exception as e:
            print("❌ Consumer thread error:", e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()


def manually_attendace_consumer():
    def manually_attendace_callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            print("📥 Received registration data:", data)

            secret = data.pop("secret", None)
            expected_secret = os.getenv("SHARED_SECRET")

            if secret != expected_secret:
                print("❌ Unauthorized registration attempt.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            payload = data.get("payload")
            if not payload:
                print("❌ Payload missing in registration data.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            from app.models import ManuallyAttendace
            attendance_data = ManuallyAttendace(**payload)

            # print("✅ Parsed student data:", attendance_data)

            try:
                conn = get_db_connection()
                insert_attendance(conn, attendance_data.reg_no, attendance_data.block_no, attendance_data.meal_slot, attendance_data.meal_cost, attendance_data.timestamp, attendance_data.date)
                conn.commit()
                conn.close()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("✅ manually attendance marked successfully")

            except Exception as e:
                print("❌ Error in manually attendance task:", e)

        except Exception as e:
            print("❌ Callback level error:", str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Prevent requeue


    def run():
        try:
            # 👇 Create a NEW connection and channel for this thread
            thread_connection = pika.BlockingConnection(params)
            thread_channel = thread_connection.channel()
            thread_channel.queue_declare(queue='manually_attendance_queue_for_script', durable=True)
            thread_channel.basic_consume(
                queue='manually_attendance_queue_for_script',
                on_message_callback=manually_attendace_callback
            )
            print("🚀 Manually attendance consumer started")
            thread_channel.start_consuming()
        except Exception as e:
            print("❌ Consumer thread error:", e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()


def delete_Student_consumer():
    def delete_student_callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            print("📥 Received registration data:", data)

            secret = data.pop("secret", None)
            expected_secret = os.getenv("SHARED_SECRET")

            if secret != expected_secret:
                print("❌ Unauthorized registration attempt.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            payload = data.get("payload")
            if not payload:
                print("❌ Payload missing in registration data.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            from app.models import DeleteStudentRequest
            delete_data = DeleteStudentRequest(**payload)

            # print("✅ Parsed student data:", attendance_data)

            try:
                conn = get_db_connection()
                delete_student(conn, delete_data.reg_no, delete_data.block_no)
                conn.commit()
                conn.close()
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("✅ delete student successfully")

            except Exception as e:
                print("❌ Error in delete student task:", e)

        except Exception as e:
            print("❌ Callback level error:", str(e))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Prevent requeue


    def run():
        try:
            # 👇 Create a NEW connection and channel for this thread
            thread_connection = pika.BlockingConnection(params)
            thread_channel = thread_connection.channel()
            thread_channel.queue_declare(queue='delete_student_queue_for_script', durable=True)
            thread_channel.basic_consume(
                queue='delete_student_queue_for_script',
                on_message_callback=delete_student_callback
            )
            print("🚀 Delete student consumer started")
            thread_channel.start_consuming()
        except Exception as e:
            print("❌ Consumer thread error:", e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
