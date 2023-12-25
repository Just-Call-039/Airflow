SELECT *
FROM suitecrm.transferred_to_other_queue
WHERE date(date) >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
AND destination_queue = 9018
order by date desc