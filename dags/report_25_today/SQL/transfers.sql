select distinct *
from suitecrm.transferred_to_other_queue
where date(date) = date(now())
and hour(date) between 0 and hour(now()) - 1
-- and date <= now() - interval 25 minute