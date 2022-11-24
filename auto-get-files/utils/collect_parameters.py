from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY
import os

DEF_START_DATE = (date.today() + relativedelta(months=-5))
DEF_END_DATE = (date.today() + relativedelta(months=-1))
def print_parameters():
    START_DATE = (os.getenv('START_DATE', DEF_START_DATE.strftime('%Y-%m-%d')))
    END_DATE = (os.getenv('END_DATE', DEF_END_DATE.strftime('%Y-%m-%d')))
    STATES = (os.getenv('STATES', 'SP'))
    
    # validate start date
    try:
        strt_dt = datetime.strptime(START_DATE,'%Y-%m-%d')
    except ValueError:
        strt_dt = DEF_START_DATE                
        pass
    # validate end date
    try:
        end_dt = datetime.strptime(END_DATE,'%Y-%m-%d')
    except ValueError:
        end_dt = DEF_END_DATE                
        pass
    
    # validate states
    if STATES == '':
        states = ['SP',]
    else:
        states = STATES.split(',')

    dates = [dt for dt in rrule(MONTHLY, dtstart=strt_dt, until=end_dt)]
    for state in states:
      for dt in dates:
          print(f'{state};{dt.year};{dt.month}')

if __name__ == "__main__":
    print_parameters()
