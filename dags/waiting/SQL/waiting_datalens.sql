with ocheredi as (select queue,
                         date,
                         project_name,
                         project
                  from (select queue,
                               date,
                               project_type,
                               project,
                               row_number() over (partition by queue order by queue, date desc) rwn,
                               case
                                   when queue = 9128 then 'DOMRU Dop'
                                   when queue in (9020, 9133, 9024, 9047, 9041, 9043) then 'MGTS'
                                   when queue in (9074) then '2com'
                                   when project = 11 and project_type = 1 then 'MTS LIDS'
                                   when project = 11 then 'MTS'
                                   when project = 10 and project_type = 1 then 'BEELINE LIDS'
                                   when project = 10 then 'BEELINE'
                                   when project = 19 and project_type = 1 then 'NBN LIDS'
                                   when project = 19 then 'NBN'
                                   when project = 3 and project_type = 1 then 'DOMRU LIDS'
                                   when project = 3 then 'DOMRU'
                                   when project = 5 and project_type = 1 then 'RTK LIDS'
                                   when project = 5 then 'RTK'
                                   when project = 6 and project_type = 1 then 'TTK LIDS'
                                   when project = 6 then 'TTK'
                                   else 'DR' end                                                project_name
                        from suitecrm.queue_project
                        where date >= '2024-09-01') as t1
                  where rwn = 1)

select call_time,
       caller_id,
       queue_num_curr,
       last_step,
       ochered,
       project_name
from suitecrm.waiter_log wl
         left join (select phone,
                           last_step,
                           REGEXP_SUBSTR(dialog, '[0-9]+') as ochered,
                           date(call_date)          as data
                    from suitecrm_robot.jc_robot_log
                    where date(call_date) >= date(now())
                    
                    union all
                    
                    select phone,
                           last_step,
                           robot_id as ochered,
                           date(call_date)          as data
                    from suitecrm_robot.robot_log 
                         left join suitecrm_robot.robot_log_addition 
                              on robot_log.id = robot_log_addition.robot_log_id
                    where date(call_date) >= date(now())) as jrl on jrl.phone = right(wl.caller_id, 11)
         left join ocheredi o on jrl.ochered = o.queue
where wl.is_parsed = 1;
