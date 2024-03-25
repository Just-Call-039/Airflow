with stoptab as (select user_id, start_status, stop_status,date(start_status) dates
    from (
    select user_id, start_status, stop_status, row_number() over (partition by user_id, date(start_status) order by stop_status desc) row
    from status_log_history join users on user_id=users.id
    where date (start_status)>='2023-04-01') tt
    where row=2
    ),

    starttab as (select user_id, start_status, date(start_status) dates
    from (
    select user_id, start_status, stop_status, row_number() over (partition by user_id, date(start_status) order by start_status) rowstart
    from status_log_history join users on user_id=users.id
    where date (start_status)>='2023-04-01' and time(start_status)>='02:00:00') tt
    where rowstart =1
    )

select starttab.user_id,starttab.start_status, stop_status
from stoptab
left join starttab on stoptab.user_id=starttab.user_id  and stoptab.dates=starttab.dates