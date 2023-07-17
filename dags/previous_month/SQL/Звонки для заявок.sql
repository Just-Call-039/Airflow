select *
from (select cl.id                                                                               as id_call,
             cl_c.asterisk_caller_id_c                                                           as phone_number,
             date(cl.date_entered)                                                               as call_date,
             cl_c.result_call_c,
             case
                 when con.city_c is null then concat(town_c, 'OBL')
                 when con.city_c in ('', ' ') then concat(town_c, 'OBL')
                 else con.city_c
                 end                                                                             as city,
             row_number() over (partition by cl_c.asterisk_caller_id_c order by cl.date_entered desc) as num,
             substring(dialog, 11, 4)                                                               queue,
             cl.assigned_user_id
      from suitecrm.calls as cl
               left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
               left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
               left join suitecrm.contacts_cstm con on con.id_c = contacts.id
               left join suitecrm_robot.jc_robot_log on asterisk_caller_id_c = phone
      where day (cl.date_entered) != day (curdate())
  and month (cl.date_entered) = month (curdate())
  and year (cl.date_entered) = year (curdate())
        and last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')) as temp
where num = 1