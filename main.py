from app.api import app
from app.rabbitmq_utils import start_feedback_consumer, start_attendance_consumer, start_registration_consumer, manually_attendace_consumer, delete_Student_consumer

start_feedback_consumer()
start_attendance_consumer()
start_registration_consumer()
manually_attendace_consumer()
delete_Student_consumer()

#uvicorn main:app --reload --host 0.0.0.0 --port 8000
