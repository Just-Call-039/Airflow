select *
from suitecrm.transferred_to_other_queue
where day(date(date)) != day(curdate())
  and month(date(date)) = month(curdate())
  and year(date(date)) = year(curdate());
