class DelayTimeseries:
    name = 'delay_timeseries'
    desc = 'prints timeseries of arrival_delay of TRAIN_NAME at STATION_NAME'
    args = ['TRAIN_NAME','STATION_NAME']

    def run(self, db, args):
      delay_timeseries = list(db.get_delay_timeseries(str(args[0]),str(args[1])))
      print('-' * 60)
      print('Delays of '+args[0]+' at '+args[1])
      print('-' * 60)
      for delay in delay_timeseries:
          print('%15s | %5s' %(str(delay['schedule_date']),str(delay['arrival_delay'])))
