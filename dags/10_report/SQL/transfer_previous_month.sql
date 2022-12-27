select *
from suitecrm.transferred_to_other_queue
where month(date(date)) = month(curdate() - interval 1 month)
  and year(date(date)) = year(curdate());
