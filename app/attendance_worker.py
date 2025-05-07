import sys
from app.face_utils import get_current_meal

import face_recognition
import cv2
from datetime import datetime
from scipy.spatial import KDTree
import time
from app.db_utils import get_db_connection, get_all_embeddings, get_student_info, has_eaten, insert_attendance
from app.face_utils import get_current_meal
from app.rabbitmq_utils import send_attendance_event_toManager, send_attendance_event_toStudent

def run_attendance_loop(block_no: str):
    cap = cv2.VideoCapture(0)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)

    conn = get_db_connection()
    all_embeddings = get_all_embeddings(conn, block_no)

    if not all_embeddings:
        print("No embeddings found.")
        return

    reg_nos, embeddings = zip(*all_embeddings)
    tree = KDTree(embeddings)

    print("Face recognition loop started...")

    last_detected = {}
    frame_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            continue

        frame_count += 1

        # Skip every 2nd frame to reduce lag
        if frame_count % 2 != 0:
            cv2.imshow("Live Attendance", frame)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
            continue

        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        boxes = face_recognition.face_locations(rgb_frame)
        encodings = face_recognition.face_encodings(rgb_frame, boxes)

        detected_any = False

        for (top, right, bottom, left), encoding in zip(boxes, encodings):
            distance, idx = tree.query(encoding)
            threshold = 0.45

            if distance > threshold:
                # Unknown face
                print("Unknown face detected.")
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)
                cv2.putText(frame, "Unknown", (left, top - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)
                detected_any = True
                cv2.imshow("Live Attendance", frame)
                cv2.waitKey(1)
                time.sleep(2)
                continue

            matched_reg_no = reg_nos[idx]

            # Avoid rapid re-detection
            if matched_reg_no in last_detected:
                if time.time() - last_detected[matched_reg_no] < 3:
                    continue

            student = get_student_info(conn, matched_reg_no, block_no)
            if not student:
                print("Student not found.")
                continue

            name_label = f"{student['reg_no']} - {student['name']}"
            cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)
            cv2.putText(frame, name_label, (left, top - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)

            current_slot, cost = get_current_meal()
            if not current_slot:
                print("Mess closed.")
                continue

            date_today = datetime.now().strftime("%Y-%m-%d")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if has_eaten(conn, matched_reg_no, student["block_no"], current_slot, date_today):
                # Already eaten
                print(f"{matched_reg_no} already eaten.")
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)
                cv2.putText(frame, "Already Eaten", (left, bottom + 25),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)
                detected_any = True
                cv2.imshow("Live Attendance", frame)
                cv2.waitKey(1)
                time.sleep(2)
                continue

            # Mark attendance
            insert_attendance(conn, matched_reg_no, student["block_no"], current_slot, cost, timestamp, date_today)

            send_attendance_event_toStudent({
                "reg_no": matched_reg_no,
                "block_no": student["block_no"],
                "meal_slot": current_slot,
                "meal_cost": cost,
                "timestamp": timestamp,
                "date": date_today
            })

            send_attendance_event_toManager({
                "reg_no": matched_reg_no,
                "block_no": student["block_no"],
                "meal_slot": current_slot,
                "meal_cost": cost,
                "timestamp": timestamp,
                "date": date_today
            })

            print(f"Attendance marked for {matched_reg_no} at {timestamp} for {current_slot}.")

            last_detected[matched_reg_no] = time.time()
            detected_any = True

            cv2.imshow("Live Attendance", frame)
            cv2.waitKey(1)
            time.sleep(2)  # Hold for manager view

        if not detected_any:
            cv2.imshow("Live Attendance", frame)
            cv2.waitKey(1)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()
    conn.close()


if __name__ == "__main__":
    block_no = sys.argv[1]  # Accept block_no as argument
    run_attendance_loop(block_no)
