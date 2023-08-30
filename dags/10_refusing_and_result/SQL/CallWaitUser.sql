select assigned_user_id, date(jc_planned_calls.date_entered) dateentered, contacts_status, date(jc_planned_calls.date_start) datestart, contact_id_c, town_c
from jc_planned_calls join users on users.id=assigned_user_id join jc_planned_calls_cstm on jc_planned_calls.id=id_c
# group by assigned_user_id,  contacts_status, contact_id_c,town_c, dateentered, datestart