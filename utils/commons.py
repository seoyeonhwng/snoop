import os
import json
from datetime import datetime, timedelta

def get_current_time(date_format=None, day_delta=None):
    today = datetime.utcnow() + timedelta(hours=9)

    if day_delta is not None:
        today += timedelta(days=day_delta)
    
    if date_format is None:
        return today
    return today.strftime(date_format)
