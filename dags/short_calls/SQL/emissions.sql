select date_entered datetime,user_id_c user ,sip_c sip, queue_c queue,
       speed_c speed,ping_c ping,type_problem_c type
from suitecrm.jc_technical_operators_problem
left join suitecrm.jc_technical_operators_problem_cstm on id = id_c
where date(date_entered) >= '2023-10-01'