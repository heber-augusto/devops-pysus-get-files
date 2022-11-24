from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY
import os


def print_parameters():
    START_DATE = (os.getenv('START_DATE', (date.today() + relativedelta(months=-6)).strftime('%Y-%m-%d')))
    END_DATE = (os.getenv('END_DATE', (date.today().strftime('%Y-%m-%d'))))
    STATES = (os.getenv('STATES', 'SP'))

    strt_dt = datetime.strptime(START_DATE,'%Y-%m-%d')
    end_dt = datetime.strptime(END_DATE,'%Y-%m-%d')
    states = STATES.split(',')

    dates = [dt for dt in rrule(MONTHLY, dtstart=strt_dt, until=end_dt)]
    for state in states:
      for dt in dates:
          print(f'{state};{dt.year};{dt.month}')

if __name__ == "__main__":
    print_parameters()
