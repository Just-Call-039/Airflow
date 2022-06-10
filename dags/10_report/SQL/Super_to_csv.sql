select *
from (select 'id', 'first_name', 'last_name', 'department_c'
      union all
      select id, first_name, last_name, department_c
      from suitecrm.users
               left join suitecrm.users_cstm on users.id = users_cstm.id_c
      where id in (select distinct supervisor from suitecrm.worktime_supervisor)) as t1
into outfile '/var/lib/mysql-files/10_report/Super.csv'
    fields terminated by ';';
