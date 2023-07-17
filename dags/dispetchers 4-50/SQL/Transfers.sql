SELECT *
FROM suitecrm.transferred_to_other_queue
WHERE date(date) = date(now()) - interval 1 day 
AND destination_queue = 90003