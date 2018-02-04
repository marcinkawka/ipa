class PrintDelays:
    name = 'print_delays'
    desc = 'prints delayed trains above given treshold and from given day ACTIVE or not (0/1)'
    args = ['MIN_DELAY','YYYY-MM-DD','ACTIVE']

    def run(self, db, args):
       trains=list(db.get_most_delayed_trains(str(args[0]),str(args[1]),str(args[2])))
       if trains == []:
          print('No trains delayed above {0} on {1}' .format(args[0],args[1]))
          return
       
       print('-' * 100)
       prompt = 'Active' if int(args[2])!=0 else 'Past'
       print(prompt + ' delays larger than {0} minutes on {1}' .format(args[0],args[1]))
       print('-' * 100)   	
       for tr in trains:
            print('%30s | %-30s | + %-3s min | %-30s' \
            % ( str(tr['train_name']),str(tr['station_name']),\
                str(tr['max_delay']),str(tr['arrival_time'])  ))

