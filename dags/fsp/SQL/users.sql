with fio as (select id, concat(first_name, ' ', last_name) fio, team,penalty_c
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
                              end team,
penalty_c
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
                    end team,
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
# where fio is not null
# and id = '45400544-e3e6-f4fb-f1dd-5c500fb13e8c'
