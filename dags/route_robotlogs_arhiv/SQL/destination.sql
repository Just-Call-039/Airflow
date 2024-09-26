-- SELECT dialog AS queue,
--        destination_queue,
--        date,
--        phone
--   FROM suitecrm.transferred_to_other_queue
--  WHERE date(date) = date(now()) - interval {n} day
--  date(now()) - interval 1 day

SELECT dialog AS queue,
       destination_queue,
       date,
       phone
  FROM suitecrm.transferred_to_other_queue
 WHERE date(date) = "2024-08-01" - interval {n} day