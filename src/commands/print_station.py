class PrintStation:
    name = 'print_station'
    desc = 'prints train schedules'
    args = ['TRAIN_STATION','YYYY-MM-DD']

    def run(self, db, args):
       station_trains=list(db.get_station_trains(str(args[0]),str(args[1])))
       if station_trains == []:
          print('No trains found on station {0} on {1}' .format(args[0],args[1]))
          return	
       print('-' * 100)
       print(args[0]+' '+args[1])
       print('-' * 100)

       for tr in station_trains:
          print('%30s | %20s | %5s min | %20s | %5s min' \
           % (tr['train_name'],str(tr['arrival_time']),str(tr['arrival_delay']),\
           str(tr['departure_time']),str(tr['departure_delay'])))
