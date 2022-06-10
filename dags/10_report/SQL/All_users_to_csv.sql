select *
from (select 'id', 'first_name', 'last_name', 'department_c'
      union all
      select id, first_name, last_name, department_c
      from suitecrm.users
               left join suitecrm.users_cstm on users.id = users_cstm.id_c) as t1
into outfile '/var/lib/mysql-files/10_report/All_users.csv'
    fields terminated by ';';
