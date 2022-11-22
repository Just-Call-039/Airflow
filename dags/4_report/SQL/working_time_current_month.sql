select id_user,
       date,
       if(talk_inbound is NULL, 0, talk_inbound)                                                 as talk_inbound,
       if(talk_outbound is NULL, 0, talk_outbound)                                               as talk_outbound,
       if((fact - talk_inbound -
           if((talk_outbound - (recall - recall_talk)) < 0, 0, (talk_outbound - (recall - recall_talk))) -
           obrabotka_in_fact - progul_obrabotka_in_fact) is NULL, 0, (fact - talk_inbound -
                                                                      if((talk_outbound - (recall - recall_talk)) < 0,
                                                                         0, (talk_outbound - (recall - recall_talk))) -
                                                                      obrabotka_in_fact -
                                                                      progul_obrabotka_in_fact)) as ozhidanie,
       if(obrabotka is NULL, 0, obrabotka)                                                       as obrabotka,
       if(training is NULL, 0, training)                                                         as training,
       if(nastavnik is NULL, 0, nastavnik)                                                       as nastavnik,
       if(sobranie is NULL, 0, sobranie)                                                         as sobranie,
       if(problems is NULL, 0, problems)                                                         as problems,
       if(obuchenie is NULL, 0, obuchenie)                                                       as obuchenie,
       if(recall_talk is NULL, 0, recall_talk)                                                   as recall_talk
from suitecrm.reports_cache
where day(date) != day(curdate())
  and month(date) = month(curdate())
  and year(date) = year(curdate())
  and id_user not in ('1', '')
  and id_user is not null;
