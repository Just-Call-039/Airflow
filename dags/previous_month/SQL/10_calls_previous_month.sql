with callsAll as (select date(calls.date_start)             dateCall,
                         assigned_user_id                   userid,
                         queue_c,
                         result_call_c,
                        if(otkaz_c = '' or otkaz_c is null,'null_status_otkaz', if(otkaz_c = '0', 'otkaz_1000', otkaz_c)) otkaz_c,
                         project_c,
                         asterisk_caller_id_c,
                         duration_minutes,
                         concat(first_name, ' ', last_name) fullname
                  from calls
                           left join calls_cstm on id = id_c
                           left join users on assigned_user_id = users.id
                  where direction = 'Inbound'
                    and month(date_start) = month(curdate() - interval 1 month)
                    and year(date_start) =
                        if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year),
                           year(curdate()))),
     robotlog as (select phone,
                         city_c,
                         assigned_user_id,
                         call_date,
                         REGEXP_SUBSTR(dialog, '[0-9]+')                                            set_queue,
                         marker,
                         case when
                                    (ptv_c like '%^3^%'
                                    or ptv_c like '%^5^%'
                                    or ptv_c like '%^6^%'
                                    or ptv_c like '%^10^%'
                                    or ptv_c like '%^11^%'
                                    or ptv_c like '%^19^%'
                                    or ptv_c like '%^14^%') then 'ptv_1'
                               when
                                   (ptv_c like '%^3_19^%'
                                   or ptv_c like '%^5_19^%'
                                   or ptv_c like '%^6_19^%'
                                   or ptv_c like '%^10_19^%'
                                   or ptv_c like '%^11_19^%'
                                   or ptv_c like '%^19_19^%'
                                   or ptv_c like '%^14_19^%'
                                   or ptv_c like '%^3_21^%'
                                   or ptv_c like '%^5_21^%'
                                   or ptv_c like '%^6_21^%'
                                   or ptv_c like '%^10_21^%'
                                   or ptv_c like '%^11_21^%'
                                   or ptv_c like '%^19_21^%'
                                   or ptv_c like '%^14_21^%'
                                   or ptv_c like '%^3_18^%'
                                   or ptv_c like '%^5_18^%'
                                   or ptv_c like '%^6_18^%'
                                   or ptv_c like '%^10_18^%'
                                   or ptv_c like '%^11_18^%'
                                   or ptv_c like '%^19_18^%'
                                   or ptv_c like '%^14_18^%'
                                   or ptv_c like '%^5_20^%'
                                   or ptv_c like '%^3_20^%'
                                   or ptv_c like '%^6_20^%'
                                   or ptv_c like '%^10_20^%'
                                   or ptv_c like '%^11_20^%'
                                   or ptv_c like '%^19_20^%'
                                   or ptv_c like '%^14_20^%'
                                   or ptv_c like '%^3_17^%'
                                   or ptv_c like '%^5_17^%'
                                   or ptv_c like '%^6_17^%'
                                   or ptv_c like '%^10_17^%'
                                   or ptv_c like '%^11_17^%'
                                   or ptv_c like '%^19_17^%'
                                   or ptv_c like '%^14_17^%'
                                   or ptv_c like '%^5_16^%'
                                   or ptv_c like '%^3_16^%'
                                   or ptv_c like '%^6_16^%'
                                   or ptv_c like '%^10_16^%'
                                   or ptv_c like '%^11_16^%'
                                   or ptv_c like '%^19_16^%'
                                   or ptv_c like '%^14_16^%'
                                   or ptv_c like '%^5_15^%'
                                   or ptv_c like '%^3_15^%'
                                   or ptv_c like '%^6_15^%'
                                   or ptv_c like '%^10_15^%'
                                   or ptv_c like '%^11_15^%'
                                   or ptv_c like '%^19_15^%'
                                   or ptv_c like '%^14_15^%') then 'ptv_2'
                                else region_c end                           as ptv
                  from suitecrm_robot.jc_robot_log
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                    and date(call_date) >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') and
                        date(call_date) < DATE_FORMAT(CURDATE(), '%Y-%m-01') 
                union all
                select   phone,
                         city city_c,
                         operator_id assigned_user_id,
                         call_date,
                         robot_id                                           set_queue,
                         marker,
                         case when
                                    (ptv like '%^3^%'
                                    or ptv like '%^5^%'
                                    or ptv like '%^6^%'
                                    or ptv like '%^10^%'
                                    or ptv like '%^11^%'
                                    or ptv like '%^19^%'
                                    or ptv like '%^14^%') then 'ptv_1'
                               when
                                   (ptv like '%^3_19^%'
                                   or ptv like '%^5_19^%'
                                   or ptv like '%^6_19^%'
                                   or ptv like '%^10_19^%'
                                   or ptv like '%^11_19^%'
                                   or ptv like '%^19_19^%'
                                   or ptv like '%^14_19^%'
                                   or ptv like '%^3_21^%'
                                   or ptv like '%^3_21^%'
                                   or ptv like '%^5_21^%'
                                   or ptv like '%^6_21^%'
                                   or ptv like '%^10_21^%'
                                   or ptv like '%^11_21^%'
                                   or ptv like '%^19_21^%'
                                   or ptv like '%^14_21^%'
                                   or ptv like '%^3_18^%'
                                   or ptv like '%^5_18^%'
                                   or ptv like '%^6_18^%'
                                   or ptv like '%^10_18^%'
                                   or ptv like '%^11_18^%'
                                   or ptv like '%^19_18^%'
                                   or ptv like '%^14_18^%'
                                   or ptv like '%^5_20^%'
                                   or ptv like '%^3_20^%'
                                   or ptv like '%^6_20^%'
                                   or ptv like '%^10_20^%'
                                   or ptv like '%^11_20^%'
                                   or ptv like '%^19_20^%'
                                   or ptv like '%^14_20^%'
                                   or ptv like '%^3_17^%'
                                   or ptv like '%^5_17^%'
                                   or ptv like '%^6_17^%'
                                   or ptv like '%^10_17^%'
                                   or ptv like '%^11_17^%'
                                   or ptv like '%^19_17^%'
                                   or ptv like '%^14_17^%'
                                   or ptv like '%^5_16^%'
                                   or ptv like '%^3_16^%'
                                   or ptv like '%^6_16^%'
                                   or ptv like '%^10_16^%'
                                   or ptv like '%^11_16^%'
                                   or ptv like '%^19_16^%'
                                   or ptv like '%^14_16^%'
                                   or ptv like '%^5_15^%'
                                   or ptv like '%^3_15^%'
                                   or ptv like '%^6_15^%'
                                   or ptv like '%^10_15^%'
                                   or ptv like '%^11_15^%'
                                   or ptv like '%^19_15^%'
                                   or ptv like '%^14_15^%') then 'ptv_2'
                                else quality end                           as ptv
                  from suitecrm_robot.robot_log 
                        left join suitecrm_robot.robot_log_addition 
                        on robot_log.id = robot_log_addition.robot_log_id
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                    and date(call_date) >= DATE_FORMAT(CURDATE() - INTERVAL 1 MONTH, '%Y-%m-01') and
                        date(call_date) < DATE_FORMAT(CURDATE(), '%Y-%m-01')            
                           
                           )

select dateCall,
       userid,
       queue_c,
       result_call_c,
       otkaz_c,
       project_c,
       city_c,
       count(asterisk_caller_id_c),
       set_queue,
       duration_minutes,
       marker,
       ptv

from (
         select distinct dateCall,
                         userid,
                         queue_c,
                         result_call_c,
                         otkaz_c,
                         project_c,
                         city_c,
                         asterisk_caller_id_c,
                         duration_minutes,
                         set_queue,
                         marker,
                         ptv
         from callsAll
                  left outer join robotlog
                                  on phone = asterisk_caller_id_c and date(dateCall) = date(call_date)) tg
group by dateCall,
         userid,
         queue_c,
         result_call_c,
         otkaz_c,
         project_c,
         city_c,
         set_queue,
         duration_minutes,
         marker,
         ptv