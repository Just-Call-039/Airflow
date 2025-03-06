with fio as (select id, concat(first_name, ' ', last_name) fio, team,penalty_c
             from (select id,
                          first_name,
                          last_name,
                          department_c,
                          REGEXP_SUBSTR(first_name, '[0-9]+') team,

penalty_c
                   from suitecrm.users
                            left join suitecrm.users_cstm on users.id = users_cstm.id_c
                   where id in (select distinct supervisor from suitecrm.worktime_supervisor)) R1),
  userrr as (SELECT distinct users.id,
                concat(first_name, ' ', last_name) fio,
                REGEXP_SUBSTR(first_name, '[0-9]+') team,

                fio.fio supervisor,
                             penalty_c
FROM suitecrm.users
         left join (select id_user, supervisor
                    from (select id_user,
                                 supervisor,
                                 date(date_start),
                                 row_number() over (partition by id_user order by date_start desc) rn
                          from suitecrm.worktime_supervisor) R
                    where rn = 1) worktime_supervisor on users.id = id_user
         left join fio on supervisor = fio.id)

select id,fio, replace(team,' ','') team, supervisor, penalty_c
from userrr

