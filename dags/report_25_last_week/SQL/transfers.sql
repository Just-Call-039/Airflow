select distinct *
from suitecrm.transferred_to_other_queue
where date(date) = date(now())
and hour(date) between (date(now()) - interval 8 day) and (date(now()) - interval - 1 day)