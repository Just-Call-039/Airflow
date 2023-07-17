select distinct *
from suitecrm.transferred_to_other_queue
where date(date) between (date(now()) - interval 30 day) and date(now())