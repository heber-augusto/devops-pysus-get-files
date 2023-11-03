from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY, YEARLY
import os

ufs = ["RO", "AC", "AM", "RR","PA",
       "AP", "TO", "MA", "PI", "CE",
       "RN", "PB", "PE", "AL", "SE",
       "BA", "MG", "ES", "RJ", "SP",
       "PR", "SC", "RS", "MS", "MT",
       "GO","DF"]


def print_parameters_monthly():
    DEF_START_DATE = (date.today() + relativedelta(months=-6))
    DEF_END_DATE = (date.today() + relativedelta(months=-1))
    START_DATE = (os.getenv('START_DATE', DEF_START_DATE.strftime('%Y-%m-%d')))
    END_DATE = (os.getenv('END_DATE', DEF_END_DATE.strftime('%Y-%m-%d')))
    STATES = (os.getenv('STATES', ','.join(ufs)))
    
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
        states = ufs
    else:
        states = STATES.split(',')

    dates = [dt for dt in rrule(MONTHLY, dtstart=strt_dt, until=end_dt)]
    result = []
    for state in states:
      for dt in dates:
          print(f'{state};{dt.year};{dt.month}')
          date_result = {
              'state':state,
              'year':dt.year,            
              'month':dt.month,              
          }
          result.append(date_result)
    return result


def print_parameters_yearly():
    DEF_START_DATE = (date.today() + relativedelta(years=-5)).replace(month=1, day = 1)
    DEF_END_DATE = (date.today() + relativedelta(years=-1)).replace(month=1, day = 1)
    START_DATE = (os.getenv('START_DATE', DEF_START_DATE.strftime('%Y-01-01')))
    END_DATE = (os.getenv('END_DATE', DEF_END_DATE.strftime('%Y-01-01')))
    STATES = (os.getenv('STATES', ','.join(ufs)))
    
    # validate start date
    try:
        strt_dt = datetime.strptime(START_DATE,'%Y-01-01')
    except ValueError:
        strt_dt = DEF_START_DATE                
        pass
    # validate end date
    try:
        end_dt = datetime.strptime(END_DATE,'%Y-01-01')
    except ValueError:
        end_dt = DEF_END_DATE                
        pass
    
    # validate states
    if STATES == '':
        states = ['SP',]
    else:
        states = STATES.split(',')

    dates = [dt for dt in rrule(YEARLY, dtstart=strt_dt, until=end_dt)]
    result = []
    for state in states:
      for dt in dates:
          print(f'{state};{dt.year};{dt.month}')
          date_result = {
              'state':state,
              'year':dt.year,            
              'month':dt.month,              
          }
          result.append(date_result)
    return result          


if __name__ == "__main__":
    freq_type = os.getenv('FREQ_TYPE', 'monthly')
    if freq_type == 'monthly':
        print_parameters_monthly()
    if freq_type == 'yearly':
        print_parameters_yearly()
