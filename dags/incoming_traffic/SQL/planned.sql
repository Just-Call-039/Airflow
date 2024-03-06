select assigned_user_id usersid, dateentered, phone, last_queue_c
from (
         select assigned_user_id,
                date(jc_planned_calls.date_entered)                                                dateentered,
                if(length(replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''), ' ',
                                  '')) <=
                   10,
                   concat(8,
                          replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''), ' ', '')),
                   concat(8,
                          right(replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''), ' ',
                                        ''), 10))) as                                              phone,
                last_queue_c,
                row_number() over (partition by phone order by jc_planned_calls.date_entered desc) row
         from jc_planned_calls
                  join users on users.id = assigned_user_id
                  join jc_planned_calls_cstm on jc_planned_calls.id = id_c
         where contacts_status is null
           and date_start < now()) tt
where row = 1
  and dateentered >= '2023-12-01'


