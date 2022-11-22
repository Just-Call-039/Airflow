select id_user,
       date,
       talk_inbound,
       talk_outbound,
       fact - talk_inbound -
       if((talk_outbound - (recall - recall_talk)) < 0, 0, (talk_outbound - (recall - recall_talk))) -
       obrabotka_in_fact - progul_obrabotka_in_fact as ozhidanie,
       obrabotka,
       training,
       nastavnik,
       sobranie,
       problems,
       obuchenie,
       recall_talk
from suitecrm.reports_cache
where date(date) between date(now()) - interval 65 day and date(now()) - interval 1 day
  and id_user not in ('1', '')
  and id_user is not null;
