from datetime import datetime, timedelta

def get_dates(start_date):
    end_date = datetime.now().date()
    delta = end_date - start_date
    return [start_date + timedelta(days=day) for day in range(delta.days + 1)]

