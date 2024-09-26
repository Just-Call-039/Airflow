SELECT calldate as date,
       phone,
       SUBSTRING(dialog, 11, 4) queue,
       city
  FROM suitecrm.address_log
 WHERE date(calldate) = date(now())