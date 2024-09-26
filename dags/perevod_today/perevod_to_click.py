def click_transfer():
    import pandas as pd
    import pymysql
    from clickhouse_driver import Client
    import datetime


    sql = f'''select fio,
       supervisor,
       penalty_c,
       penalty              penalty_current,
       name,
       round(times, 2)      times,
       effect_times,
       sum(inbound_calls)   talk_in,
       sum(outbound_calls)  talk_out,
       count(distinct my_phone_work) requests,
       sum_oz,
       sum_talk

from (select asterisk_caller_id_c,
             if(direction = 'Inbound', 1, 0)  inbound_calls,
             if(direction = 'Outbound', 1, 0) outbound_calls,
             usssser.id                       user_id,
             fio,
             supervisor,
             penalty_c,
             sip
      from suitecrm.calls
               left join suitecrm.calls_cstm on id = id_c
               left join (select id,
                                 fio,
                                 supervisor,
                                 penalty_c,
                                 asterisk_ext_c sip
                          from (SELECT distinct users.id,
                                                concat(first_name, ' ', last_name) fio,
                                                fio.fio                            supervisor
                                FROM suitecrm.users
                                         left join (select id_user, supervisor
                                                    from (select id_user,
                                                                 supervisor,
                                                                 date(date_start),
                                                                 row_number() over (partition by id_user order by date_start desc) rn
                                                          from suitecrm.worktime_supervisor) R
                                                    where rn = 1) worktime_supervisor on users.id = id_user
                                         left join (select id, concat(first_name, ' ', last_name) fio
                                                    from (select id,
                                                                 first_name,
                                                                 last_name
                                                          from suitecrm.users
                                                                   left join suitecrm.users_cstm on users.id = users_cstm.id_c
                                                          where id in (select distinct supervisor from suitecrm.worktime_supervisor)) R1) fio
                                                   on supervisor = fio.id) u
                                   left join suitecrm.users_cstm as u_c on u.id = u_c.id_c) usssser
                         on assigned_user_id = usssser.id
      where date(date_entered) = curdate()) callsAll
         left join (select *
                    from (select name,
                                 SUBSTRING_INDEX(SUBSTRING_INDEX(name, '(', -1), ')', 1) qq,
                                 exten_num,
                                 row_number() over (partition by name, exten_num)        row,
                                 penalty
                          from suitecrm.adial_campaign
                                   left join suitecrm.queue_info qi
                                             on SUBSTRING_INDEX(SUBSTRING_INDEX(name, '(', -1), ')', 1) = qi.queue_num
                          where state = 'start'
                            and adial_campaign.queue_num != 'defult') tt
                    where row = 1) compain on sip = exten_num
         left join (select user_id,
                           sum(sum_oz)                    sum_oz,
                           sum(sum_talk)                  sum_talk,
                           sum_talk / (sum_talk + sum_oz) effect_times
                    from (
                             select user_id,
                                    sum(hour(sum_talk) * 3600 +
                                        minute(sum_talk) * 60 +
                                        second(sum_talk))
                                        sum_talk,
                                    sum(hour(sum_oz) * 3600 +
                                        minute(sum_oz) * 60 +
                                        second(sum_oz))
                                        sum_oz
                             from (
                                      select user_id,
                                             if(previous_grade is null, TIMEDIFF(stop_status, start_status),
                                                null) sum_talk,
                                             if(previous_grade is not null, TIMEDIFF(previous_grade, stop_status),
                                                null) sum_oz
                                      from (
                                               select user_id,
                                                      start_status,
type,
                                                      stop_status,
                                                      if(type = 'normal_call',
                                                         lead(start_status) over (partition by user_id order by stop_status),
                                                         null) as previous_grade
                                               from status_log
                                               where
                                                     type in ('normal_call', 'talk')
                                                 and
                                                     time(start_status) >= '04:00:00' ) tt
                                  ) tt1
                             group by user_id) tt2
                    group by user_id) effect_time on effect_time.user_id = callsAll.user_id
         left join (select user_id, sum(times1) - sum(times2) times
                    from (
                             select user_id,
                                    start_status,
                                    stop_status,
                                    if(type = 'fact_available' and tt1.`option` is null,
                                       hour(TIMEDIFF(stop_status, start_status)) +
                                       minute(TIMEDIFF(stop_status, start_status)) /
                                       60 +
                                       second(TIMEDIFF(stop_status, start_status)) /
                                       3600, 0) times1,
                                    if(type = 'fact_available' and tt1.`option` = 'on_pause',
                                       hour(TIMEDIFF(stop_status, start_status)) +
                                       minute(TIMEDIFF(stop_status, start_status)) /
                                       60 +
                                       second(TIMEDIFF(stop_status, start_status)) /
                                       3600,
                                       0)       times2
                             from (select user_id,
                                          start_status,
                                          type,
                                          if(type = 'fact_available' and stop_status is null,
                                             (SELECT stop_status FROM status_log ORDER BY stop_status DESC LIMIT 1),
                                             stop_status) stop_status,
                                          `option`
                                   from status_log) tt1
                             where time(start_status) >= '04:00:00') tt
                    group by user_id
) time_user on time_user.user_id = callsAll.user_id
         left join (
    select if(length(replace(
            replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''),
            ' ',
            '')) <=
              10,
              concat(8,
                     replace(
                             replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''),
                             ' ', '')),
              concat(8,
                     right(replace(
                                   replace(replace(replace(phone_work, '-', ''), ')', ''), '(', ''),
                                   ' ',
                                   ''), 10))) my_phone_work,
           user
    from (select phone_work,
                 date(r.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_rostelecom as r
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(b.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_beeline as b
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(d.date_entered) as request_date,
                 assigned_user_id     as user

          from suitecrm.jc_meetings_domru as d
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(t.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_ttk as t
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(n.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_netbynet as n
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(m.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_mts as m
          where status != 'Error'
            and status != 'doubled'
            and status != 'change_flat'
            and date(date_entered) = date(now())
          union all
          select phone_work,
                 date(o.date_entered) as request_date,
                 assigned_user_id     as user
          from suitecrm.jc_meetings_other as o
          where status != 'Error'
            and status != 'doubled'
            and date(date_entered) = date(now())) requests
    where request_date = date(now())) reqq
                   on callsAll.user_id = user and my_phone_work = asterisk_caller_id_c

group by fio,
         supervisor,
         penalty_c,
         penalty,
         name,
         times,
         effect_times, sum_oz, sum_talk'''

    print('Подключаемся к mysql')
    dest = '/root/airflow/dags/not_share/cloud_my_sql_128.csv'
    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host2 = second
                elif first == 'user':
                    user2 = second
                elif first == 'password':
                    password2 = second

    Con = pymysql.Connect(host=host2, user=user2, passwd=password2, db="suitecrm",
                        charset='utf8')
    df_full = pd.read_sql_query(sql, Con)
    current_date = datetime.datetime.now()
    yesterday_date = current_date - datetime.timedelta(days=0)
    df_full['date'] = yesterday_date.strftime('%Y-%m-%d')
    df_full['penalty_c'] = df_full['penalty_c'].astype('str').apply(lambda x: x.replace('.0',''))
    df_full['penalty_current'] = df_full['penalty_current'].astype('str').apply(lambda x: x.replace('.0',''))
    df_full[['talk_in','talk_out',
             'requests', 'sum_oz','sum_talk']] = df_full[['talk_in','talk_out',
                                                          'requests', 'sum_oz','sum_talk']].fillna(0).astype('int64')
    df_full[['times','effect_times']] = df_full[['times','effect_times']].fillna(0).astype('float64')

    df_full[['fio','supervisor','penalty_c',
             'penalty_current','name','times',
             'effect_times','talk_in','talk_out','requests','sum_oz','sum_talk','date']]


    print('Подключаемся к clickhouse')
    dest = '/root/airflow/dags/not_share/ClickHouse2.csv'
    if dest:
                with open(dest) as file:
                    for now in file:
                        now = now.strip().split('=')
                        first, second = now[0].strip(), now[1].strip()
                        if first == 'host':
                            host = second
                        elif first == 'user':
                            user = second
                        elif first == 'password':
                            password = second
        # return host, user, password

    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})
    
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.perevod_today VALUES', df_full)

    