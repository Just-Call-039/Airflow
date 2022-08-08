select *
from (select 'id', 'date_entered', 'first_workday_c', 'dismissal_date_c', 'last_login_c', 'user_id_c'
      union all
      select id, date_entered, first_workday_c, dismissal_date_c, last_login_c, user_id_c
      from suitecrm.users as u
               left join suitecrm.users_cstm as u_c on u.id = u_c.id_c) as t_1
into outfile '/var/lib/mysql-files/4_report/Users_total.csv'
    fields terminated by ';';
