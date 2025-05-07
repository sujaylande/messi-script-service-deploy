from pydantic import BaseModel
from typing import Optional

class RegisterStudent(BaseModel):
    name: str
    email: str
    reg_no: str
    roll_no: str
    password: str
    block_no: str

class AttendanceRequest(BaseModel):
    block_no: str

class ManuallyAttendace(BaseModel):
    reg_no: str
    date: str
    meal_slot: str
    meal_cost: str
    block_no: str
    timestamp: str

class DeleteStudentRequest(BaseModel):
    reg_no: str
    block_no: str


