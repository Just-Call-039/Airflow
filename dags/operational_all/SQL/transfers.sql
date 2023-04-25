select date(date) date, dialog, count(phone)
from suitecrm.transferred_to_other_queue
where date(date) >= '2022-06-01'
group by 1,2

