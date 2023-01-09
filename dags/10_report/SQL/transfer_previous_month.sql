select *
from suitecrm.transferred_to_other_queue
where month(date(date)) = month(curdate() - interval 1 month)
  and year(date(date)) =
      if(month(curdate() - interval 1 month) = 12, year(curdate() - interval 1 year), year(curdate()));
