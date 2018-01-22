import mysql.connector
import ipa_db_schema

class DbError(Exception):
    def __init__(self, message):
        super(DbError, self).__init__(message)

class Db:
    def __init__(self, db_config):
        db_config['buffered'] = True
        self.conn = mysql.connector.connect(**db_config)

    def __del__(self):
        self.conn.close()

    def _execute(self, sql, args = tuple()):
        c = self.conn.cursor()
        try:
            c.execute(sql, args)
        except Exception as e:
            print(e)
            raise DbError(str(e))
        else:
            return c

    def _format_select(self, cursor):
        names = [desc[0] for desc in cursor.description]
        for row in cursor:
            yield dict(zip(names, row))

    def commit(self):
        self.conn.commit()

    def remove_schema(self):
        for table in ipa_db_schema.tables:
            self._execute(table.drop)

    def create_schema(self):
        for table in ipa_db_schema.tables:
            self._execute(table.create)

    def select_query(self, query, args = tuple()):
        return self._format_select(self._execute(query, args))

    def get_trains(self):
        return self.select_query('''SELECT train_name, GROUP_CONCAT(station_name ORDER BY stop_number) AS stations
                                    FROM schedule_info
                                    JOIN (SELECT train_name, MAX(schedule_id) AS last_schedule_id
                                          FROM schedule JOIN train USING (train_id) GROUP BY train_id) t
                                        ON t.last_schedule_id = schedule_info.schedule_id
                                    JOIN station USING (station_id)
                                    GROUP BY train_name ORDER BY train_name''')

    def get_active_schedules(self):
        return self.select_query('SELECT schedule_id FROM schedule WHERE active = 1')

    def set_active(self, schedule_id, active):
        active_value = 1 if active else 0
        self._execute('''UPDATE schedule SET active = %s WHERE schedule_id = %s''', (active_value, schedule_id))

    def add_train(self, train_name):
        self._execute('''INSERT INTO train VALUES('', %s)''', (train_name,))

    def get_train_id(self, train_name):
        return self.select_query('SELECT train_id, train_name FROM train WHERE train_name = %s', (train_name,))

    def add_station(self, station_name):
        self._execute('''INSERT INTO station VALUES('', %s)''', (station_name,))

    def get_station_id(self, station_name):
        return self.select_query('SELECT station_id FROM station WHERE station_name = %s', (station_name,))
	
    def get_station_trains(self,station_name,date):
        return self.select_query('''SELECT sinf.arrival_time,sinf.departure_time,sinf.arrival_delay,sinf.departure_delay,tr.train_name 
										FROM schedule_info sinf 
									INNER JOIN schedule sc on sinf.schedule_id=sc.schedule_id
									INNER JOIN train tr on sc.train_id=tr.train_id	
										WHERE sinf.station_id=(
											SELECT st.station_id FROM station st WHERE st.station_name=%s)
										AND DATE(sinf.arrival_time) =%s
									ORDER BY (sinf.arrival_time) ''',(station_name,date))
	
    def get_most_delayed_trains(self,min_delay,date):
        return self.select_query('''select 	t2.max_delay,
                                            tr.train_name,
                                            s_info2.arrival_time,
                                            st.station_name
                                    from train tr,
                                         schedule_info s_info2,
                                         schedule s2,
                                         station st
	                                inner join
                                        (select s.schedule_id,
                                                s.train_id,
                                                max(t1.arrival_delay) as max_delay,
                                                t1.station_id
                                        from schedule s
                                        inner join
                                            (select  s_info.arrival_delay,
                                                     s_info.arrival_time,
                                                     s_info.schedule_id,
                                                     s_info.station_id
                                            from schedule_info s_info
                                            ) t1 on s.schedule_id=t1.schedule_id
                                        where DATE(t1.arrival_time)=%s
                                        and t1.arrival_delay> %s
                                        group by s.train_id) t2
    
                                    where tr.train_id=t2.train_id
                                        and s_info2.arrival_delay=t2.max_delay
                                        and s_info2.schedule_id=s2.schedule_id
                                        and st.station_id=s_info2.station_id
                                        and s_info2.station_id=t2.station_id
                                        and DATE(s_info2.arrival_time)=%s
                                    group by tr.train_id    
                                order by max_delay desc''',(date,min_delay,date))

    	
    def update_schedule(self, schedule_id, schedule_date, train_id):
        self._execute('''REPLACE INTO schedule VALUES(%s, %s, %s, 1)''', (schedule_id, schedule_date, train_id))

    def get_schedules(self, train_id):
        return self.select_query('''SELECT schedule_id, schedule_date FROM schedule INNER JOIN train USING (train_id)
                                    WHERE schedule.train_id = %s ORDER BY schedule_date DESC''', (train_id,))


    def update_schedule_info(self, schedule_id, stop_number, station_id, info):
        self._execute('''REPLACE INTO schedule_info VALUES
                         (%s, %s, %s, %s, %s, %s, %s)''',
                         (schedule_id, stop_number, station_id,
                          info['arrival_time'], info['arrival_delay'],
                          info['departure_time'], info['departure_delay']))

    def get_schedule_infos(self, schedule_id):
        return self.select_query('''SELECT station_name, departure_time, departure_delay, arrival_time, arrival_delay
                                    FROM schedule_info INNER JOIN station USING (station_id)
                                    WHERE schedule_id = %s ORDER BY stop_number''', (schedule_id,))
