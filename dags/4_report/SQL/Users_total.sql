select id,
       date_entered,
       first_workday_c,
       dismissal_date_c,
       last_login_c,
       user_id_c,
       penalty_c,
       asterisk_ext_c as sip
from suitecrm.users as u
         left join suitecrm.users_cstm as u_c on u.id = u_c.id_c;
