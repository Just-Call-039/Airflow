
with work_time as (select rc.id_user,
                          rc.date,
                          row_number() over (partition by rc.id_user, rc.date)                                               as num,
                          if(rc.talk_inbound is null, 0, rc.talk_inbound)                                                    as talk_inbound,
                          if(rc.talk_outbound is null, 0, rc.talk_outbound)                                                  as talk_outbound,
                          if((rc.fact - rc.talk_inbound -
                              if((rc.talk_outbound - (rc.recall - rc.recall_talk)) < 0, 0,
                                 (rc.talk_outbound - (rc.recall - rc.recall_talk))) -
                              rc.obrabotka_in_fact - rc.progul_obrabotka_in_fact) is null, 0, (rc.fact - talk_inbound -
                                                                                               if(
                                                                                                           (rc.talk_outbound - (rc.recall - rc.recall_talk)) <
                                                                                                           0,
                                                                                                           0,
                                                                                                           (rc.talk_outbound - (rc.recall - rc.recall_talk))) -
                                                                                               rc.obrabotka_in_fact -
                                                                                               rc.progul_obrabotka_in_fact)) as ozhidanie,
                          if(rc.obrabotka is null, 0, rc.obrabotka)                                                          as obrabotka,
                          if(rc.training is null, 0, rc.training)                                                            as training,
                          if(rc.nastavnik is null, 0, rc.nastavnik)                                                          as nastavnik,
                          if(rc.sobranie is null, 0, rc.sobranie)                                                            as sobranie,
                          if(rc.problems is null, 0, rc.problems)                                                            as problems,
                          if(rc.obuchenie is null, 0, rc.obuchenie)                                                          as obuchenie,
                          if(rc.recall is null, 0, rc.recall)                                                                as dorabotka,
                          if(rc.recall is null, 0, rc.recall_talk)                                                           as dorabotka_talk,
                          if(rc.pause10 is null, 0, rc.pause10)                                                              as pause,
                          if(timestampdiff(second, wt.lunch_start, wt.lunch_stop) is null, 0,
                             timestampdiff(second, wt.lunch_start, wt.lunch_stop))                                           as lunch_duration,
                          user_name
                   from suitecrm.reports_cache as rc
                            left join suitecrm.worktime_log as wt
                                      on rc.id_user = wt.id_user and date(rc.date) = date(wt.date)
                            left join users on rc.id_user = users.id
                   where  date(rc.date) = date(now()) -interval 1 day
                     and rc.id_user not in ('1', '')
                     and rc.id_user is not null),

     stoptab as (select user_id, start_status, stop_status, date(start_status) dates
                 from (
                          select user_id,
                                 start_status,
                                 stop_status,
                                 row_number() over (partition by user_id, date(start_status) order by stop_status desc) row
                          from status_log_history
                                   join users on user_id = users.id
                          where (date(stop_status) >= '2023-10-01'
                            and time(stop_status) <= '21:00:00')) tt
                 where row = 1
     ),

     starttab as (select user_id, start_status, date(start_status) dates
                  from (
                           select user_id,
                                  start_status,
                                  stop_status,
                                  row_number() over (partition by user_id, date(start_status) order by start_status) rowstart
                           from status_log_history
                                    join users on user_id = users.id
                           where date(start_status) >= '2023-10-01'
                             and time(start_status) >= '02:00:00') tt
                  where rowstart = 1
     ),

     fulllogin as (select starttab.user_id, starttab.dates, starttab.start_status, stop_status
                   from stoptab
                            left join starttab on stoptab.user_id = starttab.user_id and stoptab.dates = starttab.dates)


select work_time.id_user,
       work_time.user_name,
       work_time.date,
       work_time.talk_inbound,
       work_time.talk_outbound,
       work_time.ozhidanie,
       work_time.obrabotka,
       work_time.training,
       work_time.nastavnik,
       work_time.sobranie,
       work_time.problems,
       work_time.obuchenie,
       work_time.dorabotka,
       work_time.pause,
       work_time.lunch_duration,
       work_time.dorabotka_talk,
       start_status,
       stop_status,
       DATE_FORMAT(start_status, '%H:%i:%s') time_start_status,
       DATE_FORMAT(stop_status, '%H:%i:%s') time_stop_status
from work_time
         left join fulllogin on user_id = id_user and date(date) = date(dates)
where work_time.num = 1
order by work_time.date, work_time.id_user ;
