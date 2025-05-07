from fastapi import FastAPI, HTTPException
from app.models import RegisterStudent, AttendanceRequest
from app.db_utils import get_db_connection, insert_student, insert_embedding
from app.rabbitmq_utils import send_registration_event_toManager, send_registration_event_toStudent, send_registration_Status_toManager
import subprocess
import logging
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from app.face_utils import capture_face
from fastapi import BackgroundTasks
import requests

from fastapi import Query
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import date
from fastapi import Header
import os

# Download NLTK resources
nltk.download('vader_lexicon')


logger = logging.getLogger(__name__)

app = FastAPI()

# Custom exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "body": await request.body()
        },
    )


# def process_registration(data: RegisterStudent):
#     try:
#         embedding = capture_face()
#         conn = get_db_connection()
#         insert_student(conn, data.dict())
#         insert_embedding(conn, data.reg_no, data.block_no, embedding)
#         send_registration_event_toManager(data.dict())
#         send_registration_event_toStudent(data.dict())
#         send_registration_Status_toManager({
#             "reg_no": data.reg_no,
#             "status": "success"
#         })
#         conn.close()
#     except Exception as e:
#         # On failure
#         send_registration_Status_toManager({
#             "reg_no": data.reg_no,
#             "status": "fail"
#         })
#         logger.error(f"Error in background registration task: {e}", exc_info=True)


# @app.post("/register")
# async def register_student(data: RegisterStudent, background_tasks: BackgroundTasks):
#     try:
#         # Queue the registration logic as a background task
#         background_tasks.add_task(process_registration, data)
#         return {"message": "Registration started"}
#     except Exception as e:
#         logger.error(f"Error during registration: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="Internal server error during registration")
    

# @app.get("/forecast/{block_no}")
# def forecast_attendance(block_no: int):
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor(dictionary=True)
#         query = """
#             SELECT date, meal_slot, COUNT(*) as count 
#             FROM attendance 
#             WHERE block_no = %s
#             GROUP BY date, meal_slot
#         """
#         cursor.execute(query, (block_no,))
#         rows = cursor.fetchall()
#         conn.close()
#         cursor.close()

#         if not rows:
#             return []

#         df = pd.DataFrame(rows)
#         df['date'] = pd.to_datetime(df['date'])
#         df.set_index('date', inplace=True)

#         pivot_df = df.pivot_table(index='date', columns='meal_slot', values='count', aggfunc='sum').fillna(0)
#         pivot_df = pivot_df.asfreq('D', fill_value=0)

#         forecast_data = {}
#         for meal in pivot_df.columns:
#             if pivot_df[meal].count() < 2:
#                 forecast_data[meal] = []
#                 continue

#             try:
#                 model = ExponentialSmoothing(pivot_df[meal], trend="add", seasonal=None).fit()
#                 future_forecast = model.forecast(7)
#                 forecast_data[meal] = future_forecast.tolist()
#             except Exception as e:
#                 forecast_data[meal] = []

#         return forecast_data

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/{block_no}")
def forecast_attendance(block_no: int, secret: str = Header(None)):
    expected_secret = os.getenv("SHARED_SECRET")
    if secret != expected_secret:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid secret")

    try:
        conn = get_db_connection()
        # print("conn", conn)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT date, meal_slot, COUNT(*) as count 
            FROM attendance 
            WHERE block_no = %s
            GROUP BY date, meal_slot
        """
        cursor.execute(query, (block_no,))
        rows = cursor.fetchall()
        conn.close()
        cursor.close()

        if not rows:
            return []

        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        pivot_df = df.pivot_table(index='date', columns='meal_slot', values='count', aggfunc='sum').fillna(0)
        pivot_df = pivot_df.asfreq('D', fill_value=0)

        forecast_data = {}
        for meal in pivot_df.columns:
            if pivot_df[meal].count() < 2:
                forecast_data[meal] = []
                continue

            try:
                model = ExponentialSmoothing(pivot_df[meal], trend="add", seasonal=None).fit()
                future_forecast = model.forecast(7)
                forecast_data[meal] = future_forecast.tolist()
            except Exception as e:
                forecast_data[meal] = []

        return forecast_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @app.post("/attendance")
# async def start_attendance(request: AttendanceRequest):
#     try:
#         print("Launching worker as module: app.attendance_worker")
#         subprocess.Popen(["python", "-m", "app.attendance_worker", request.block_no])
#         return {"message": f"Attendance process started for block: {request.block_no}"}
#     except Exception as e:
#         print("Subprocess error:", e)
#         return {"error": str(e)}



def fetch_feedback(block_no: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT id, comments FROM feedback 
        WHERE DATE(feedback_date) = CURDATE() 
        AND comments IS NOT NULL AND comments != '' 
        AND block_no = %s;
        """
        cursor.execute(query, (block_no,))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        print("fe", rows)
        return rows
    except Exception as e:
        return str(e)

def analyze_feedback(feedback_data):
    sia = SentimentIntensityAnalyzer()
    negative_comments = []

    for feedback in feedback_data:
        score = sia.polarity_scores(feedback["comments"])["compound"]
        if score <= 0:
            negative_comments.append({
                "id": feedback["id"],
                "comment": feedback["comments"],
                "score": score
            })
    
    negative_comments.sort(key=lambda x: x["score"])
    print("ne", negative_comments)
    return negative_comments

# @app.get("/feedback/negative")
# def get_negative_feedback(block_no: str = Query(...)):
#     feedback_data = fetch_feedback(block_no)
#     if isinstance(feedback_data, str):
#         raise HTTPException(status_code=500, detail=feedback_data)

#     return analyze_feedback(feedback_data)


@app.get("/feedback/negative")
def get_negative_feedback(
    block_no: str = Query(...),
    secret: str = Header(None)
):
    expected_secret = os.getenv("SHARED_SECRET")
    if secret != expected_secret:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid secret")

    feedback_data = fetch_feedback(block_no)
    if isinstance(feedback_data, str):
        raise HTTPException(status_code=500, detail=feedback_data)

    return analyze_feedback(feedback_data)
