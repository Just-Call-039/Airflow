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
                             timestampdiff(second, wt.lunch_start, wt.lunch_stop))                                           as lunch_duration
                   from suitecrm.reports_cache as rc
                            left join suitecrm.worktime_log as wt
                                      on rc.id_user = wt.id_user and date(rc.date) = date(wt.date)
                   where day(rc.date) != day(curdate())
                     and month(rc.date) = month(curdate())
                     and year(rc.date) = year(curdate())
                     and rc.id_user not in ('1', '')
                     and rc.id_user is not null)

select work_time.id_user,
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
       work_time.dorabotka_talk
from work_time
where work_time.num = 1
order by work_time.date, work_time.id_user;
