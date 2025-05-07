import cv2
import numpy as np
import face_recognition
import os
import time
from datetime import datetime


def capture_face():
    cap = cv2.VideoCapture(0)
    face_data = []

    while len(face_data) < 5:
        ret, frame = cap.read()
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        boxes = face_recognition.face_locations(rgb_frame)
        encodings = face_recognition.face_encodings(rgb_frame, boxes)

        if encodings:
            face_data.append(encodings[0])

        cv2.imshow("Register Face", frame)
        if cv2.waitKey(1) == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()
    return np.mean(face_data, axis=0)


def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


def get_current_meal():
    now = datetime.now().strftime("%H:%M")
    slots = {
        "Breakfast": ("09:08", "09:59", 50),
        "Lunch": ("10:00", "11:00", 100),
        "Snack": ("11:01", "18:59", 50),
        "Dinner": ("19:01", "20:00", 100)
    }
    for meal, (start, end, cost) in slots.items():
        if start <= now <= end:
            return meal, cost
    return None, 0
