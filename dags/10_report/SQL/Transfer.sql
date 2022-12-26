select *
from suitecrm.transferred_to_other_queue
where date(date) between date(now()) - interval 65 day and date(now());
