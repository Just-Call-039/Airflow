SELECT calldate as date,
       phone,
       SUBSTRING(dialog, 11, 4) queue,
       city
  FROM suitecrm.address_log
 WHERE date(calldate) = date(now()) - interval {n} day
--  date(now()) - interval 1 day
 