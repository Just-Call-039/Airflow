select *
from (select 'user', 'queue', 'ref', 'result_call_c', 'calls_date', 'super'
      union all
      select user,
             queue,
             ref,
             result_call_c,
             calls_date,
             super
      from (select case
                       when cl_1.assigned_user_id is null or cl_1.assigned_user_id = '' or cl_1.assigned_user_id = ' '
                           then 'unknown_id'
                       else cl_1.assigned_user_id end                                                  as user,
                   case
                       when cl.queue_c is null or cl.queue_c = '' or cl.queue_c = ' ' then 'unknown_queue'
                       else cl.queue_c end                                                             as queue,
                   cl.otkaz_c                                                                          as ref,
                   date(cl_1.date_entered)                                                             as calls_date,
                   date_entered,
                   case
                       when cl.user_id_c is null or cl.user_id_c = '' or cl.user_id_c = ' ' then 'unknown_id'
                       else cl.user_id_c end                                                           as super,
                   result_call_c,
                   cl.asterisk_caller_id_c,
                   row_number() over (partition by cl.asterisk_caller_id_c order by date_entered desc) as rwn
            from suitecrm.calls_cstm as cl
                     left join suitecrm.calls as cl_1 on cl.id_c = cl_1.id
            where cl_1.date_entered >= date(now()) - interval 31 day
               # and user_id_c is not null
           ) as t1
      where rwn = 1) as t1
into outfile '/var/lib/mysql-files/10_report/Total_calls_31d.csv'
    fields terminated by ';';
