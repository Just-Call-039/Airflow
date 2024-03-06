with fio as (select id, concat(first_name, ' ', last_name) fio, team
             from (select id,
                          first_name,
                          last_name,
                          department_c,
                          case
                              when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                  then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                              when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                  then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                              else
                                  (case
                                       when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                            left(first_name, instr(first_name, ' ') - 1) < 10000
                                           then left(first_name, instr(first_name, ' ') - 1)
                                       when left(first_name, 2) = '�_'
                                           then substring(first_name, 3, (instr(first_name, ' ') - 3))
                                       when left(first_name, 1) = '�'
                                           then substring(first_name, 2, (instr(first_name, ' ') - 1))
                                       else '' end)
                              end team
                   from suitecrm.users
                            left join suitecrm.users_cstm on users.id = users_cstm.id_c
                   where id in (select distinct supervisor from suitecrm.worktime_supervisor)) R1),
     userrr as (SELECT distinct users.id,
                                concat(first_name, ' ', last_name) fio,
                                case
                                    when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                        then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                                    when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                        then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                                    else
                                        (case
                                             when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                                  left(first_name, instr(first_name, ' ') - 1) < 10000
                                                 then left(first_name, instr(first_name, ' ') - 1)
                                             when left(first_name, 2) = '�_'
                                                 then substring(first_name, 3, (instr(first_name, ' ') - 3))
                                             when left(first_name, 1) = '�'
                                                 then substring(first_name, 2, (instr(first_name, ' ') - 1))
                                             else '' end)
                                    end                            team,
                                fio.fio                            supervisor
                FROM suitecrm.users
                         left join (select id_user, supervisor
                                    from (select id_user,
                                                 supervisor,
                                                 date(date_start),
                                                 row_number() over (partition by id_user order by date_start desc) rn
                                          from suitecrm.worktime_supervisor) R
                                    where rn = 1) worktime_supervisor on users.id = id_user
                         left join fio on supervisor = fio.id),

     usersss as (select id, fio, replace(team, ' ', '') team, supervisor
                 from userrr)


select distinct TT.*
from (select direction,
             calls.assigned_user_id,
             fio,
             if(length(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                               '')) <=
                10,
                concat(8,
                       replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ', '')),
                concat(8,
                       right(replace(replace(replace(replace(asterisk_caller_id_c, '-', ''), ')', ''), '(', ''), ' ',
                                     ''), 10))) as phone ,
             date(calls.date_entered)              calldate,
             queue_c
      from suitecrm.calls
               left join suitecrm.calls_cstm on id = id_c
               left join usersss on assigned_user_id = usersss.id
      where DATE_FORMAT(calls.date_entered, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')
  AND calls.date_entered < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
        and direction = 'Inbound'
        and team not in ('12', '4')
        and result_call_c = 'refusing'
        and (otkaz_c = 'otkaz_23' or otkaz_c = 'otkaz_42' or otkaz_c = 'no_answer')
        and duration_minutes <= 10
     ) TT

