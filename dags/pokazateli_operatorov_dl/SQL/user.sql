-- Скрипт по выгрузке данных по операторам, их имени, супервайзера, id и команды
-- джойним с worktime_supervisor, чтобы вытащить supervisor и users_cstm, чтобы достать пенальти, сип и тд


    with super as (
					select id,
						   CONCAT(first_name, ' ', last_name) as fio,
					       REGEXP_REPLACE(first_name, '[^0-9]', '')  as team

   		  	  		  from suitecrm.users 
   
   				 	 where id in (select distinct supervisor from suitecrm.worktime_supervisor)
   				   ),
   				 	
    operator as ( select   u.id,
						   CONCAT(u.first_name, ' ', u.last_name) as fio,
					       REGEXP_REPLACE(REGEXP_REPLACE(u.first_name, 'T2', ''), '[^0-9]', '')  as team,
					       super.fio as supervisor,
						   uc.first_workday_c as first_workday,  
						   uc.penalty_c as penalty,
						   uc.asterisk_ext_c as sip

					  from suitecrm.users u

					  left join suitecrm.users_cstm as uc
					   
					  	   on u.id = uc.id_c

					  left join 
					  			(select id_user,
                                        supervisor,
                                        date(date_start),
                                        row_number() over (partition by id_user order by date_start desc) rn
                                   from suitecrm.worktime_supervisor)  ws       
                           on ws.id_user = u.id and rn = 1

					  left join super
					  	   on ws.supervisor = super.id
					 )
					 
select id as user_id,
	   fio as operator,
	   team,
	   supervisor,
	   first_workday,  
	   penalty,
	   sip
  from operator
   				 
   				 
   				 
