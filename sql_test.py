import pandas as pd
import psycopg2
from tabulate import tabulate


SETTINGS = {"dbname": "data",
            "user": "elena_roshchina",
            "host": "5.189.224.160",
            "password": "WkbFqM5gljop",
            "port": "15432"}

TABLE_NAMES = ['sessions', 'communications']

FIELDS = ['communication_id',
          'site_id',
          'visitor_id',
          'communication_date_time',
          'visitor_session_id',
          'session_date_time',
          'campaign_id',
          'row_n']


class MaxDB():
    def __init__(self, db_settings):
        connection = """dbname ='{}' user = '{}' host ='{}' password ='{}' 
                        port='{}'""".format(db_settings.get("dbname"),
                                            db_settings.get("user"),
                                            db_settings.get("host"),
                                            db_settings.get("password"),
                                            db_settings.get("port"))

        self.conn = psycopg2.connect(connection)
        self.cursor = self.conn.cursor()

    def get_conn_cursor(self):
        return self.conn, self.cursor

    def close_connection(self):
        self.conn.close()


def query_result_to_dict(field_list, query_result):
    list_of_dict = []
    for line in query_result:
        new_dict = {}
        for i in range(len(line)):
            new_dict[field_list[i]] = line[i]
        list_of_dict.append(new_dict)
    return list_of_dict


def list_to_line_with_sep(some_list, sep):
    if isinstance(some_list, list):
        line = ''
        for i in range(len(some_list)):
            line += some_list[i] + sep
        return line[:-len(sep)]
    else:
        return False

def get_tables_content():
    table_cols = {}
    db = MaxDB(db_settings=SETTINGS)
    for name in TABLE_NAMES:
        query = """select column_name from information_schema.columns where table_name='{}';""".format(name)
        db.cursor.execute(query)
        result = db.cursor.fetchall()
        if result is not None and len(result) > 0:
            table_cols[name] = [x[0] for x in result]
    tables_content = {}
    for tname in table_cols.keys():
        query = """select {} from {}""".format(list_to_line_with_sep(some_list=table_cols.get(tname), sep=', '), tname)
        db.cursor.execute(query)
        result = db.cursor.fetchall()
        tables_content[tname] = query_result_to_dict(field_list=table_cols.get(tname), query_result=result)
    db.close_connection()
    return tables_content


if __name__ == "__main__":
    # Часть А: формирование SQL-запроса
    query = """select communication_id, site_id, visitor_id,  c.date_time, visitor_session_id, 
                s.date_time, campaign_id, row_n from communications c left join 
                (select *, row_number() over(
	              partition by visitor_id, site_id order by visitor_id, site_id, date_time
                  ) as row_n from sessions) s using (visitor_id, site_id) 
                where visitor_session_id is null or visitor_session_id=(
    	            select visitor_session_id from sessions 
    	            where site_id=c.site_id and visitor_id=c.visitor_id and date_time < c.date_time 
    	            order by date_time DESC limit 1);"""

    db = MaxDB(SETTINGS)
    db.cursor.execute(query)
    query_result = db.cursor.fetchall()
    if query_result is not None and len(query_result) > 0:
        df = pd.DataFrame(query_result_to_dict(field_list=FIELDS, query_result=query_result))
        print('Часть А: решение задачи средствами SQL')
        print(tabulate(df, headers=['communication_id', 'site_id', 'visitor_id', 'date_time_c',
                                        'visitor_session_id', 'date_time', 'campaign_id', 'row_n']))
    db.close_connection()

    # Часть B: решение задачи средствами pandas

    content = get_tables_content()
    session_df = pd.DataFrame(content.get(TABLE_NAMES[0]))
    communication_df = pd.DataFrame(content.get(TABLE_NAMES[1])).rename(columns={'date_time': 'date_time_c'})
    communication_df['communication'] = communication_df['communication_id']

    fields = ['visitor_id', 'site_id']
    df = session_df.sort_values(by=['site_id', 'visitor_id', 'date_time'])
    df['row_n'] = df.groupby(fields).cumcount() + 1
    res_df = pd.merge(communication_df, df, how='left', left_on=fields, right_on=fields)

    res_df = res_df.where(res_df['date_time'] < res_df['date_time_c']).groupby('communication').max()
    res_df.visitor_session_id = res_df.visitor_session_id.astype('int64')
    res_df.site_id = res_df.site_id.astype('int64')
    res_df.visitor_id = res_df.visitor_id.astype('int64')
    res_df.communication_id = res_df.communication_id.astype('int64')
    res_df.reset_index(inplace=True, drop=True)
    print('\nЧасть B: решение задачи средствами pandas')
    print(tabulate(res_df, headers=['communication_id', 'site_id', 'visitor_id', 'date_time_c',
                                    'visitor_session_id', 'date_time', 'campaign_id', 'row_n']))
    # результаты проверены. Одно и то же.





