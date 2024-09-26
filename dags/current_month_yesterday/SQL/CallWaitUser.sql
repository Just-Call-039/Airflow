select assigned_user_id,
       date(jc_planned_calls.date_entered) dateentered,
       contacts_status,
       date(jc_planned_calls.date_start) datestart,
       contact_id_c,
       town_c,
       last_queue_c
  from jc_planned_calls
       join users on users.id=assigned_user_id
       join jc_planned_calls_cstm on jc_planned_calls.id=id_c
