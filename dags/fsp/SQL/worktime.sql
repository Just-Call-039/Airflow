select `id_user`, `date`, sum(worktime) as worktime,
       sum(waiting) waiting, sum(talk_inbound) talk_inbound
from (
SELECT `id_user`, `date`, (`recall`+ `sobranie`+ `obuchenie`+ `training`+ `nastavnik`+ `problems`+ `fact` + `pause10`- `progul_obrabotka_in_fact`) worktime,
       fact - talk_inbound - if((talk_outbound - (recall - recall_talk)) < 0, 0, (talk_outbound - (recall - recall_talk))) - obrabotka_in_fact - progul_obrabotka_in_fact  waiting,

       talk_inbound
FROM suitecrm.reports_cache
where (date between '2022-09-01' and now())
and id_user not in ('1','')
and id_user is not null    ) t
group by `id_user`, `date`







