from dataclasses import dataclass
from datetime import datetime

@dataclass
class CleanroomLog:
    particles: int
    temperature: float
    humidity: float
    pressure: float
    door_status: str
    user_id: str
    attack_status: str
    timestamp: datetime
