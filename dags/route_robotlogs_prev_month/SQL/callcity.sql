
SELECT calldate as date,
       phone,
       SUBSTRING(dialog, 11, 4) queue,
       city
  FROM suitecrm.address_log
 WHERE DATE_FORMAT(calldate, '%Y-%m') = DATE_FORMAT({date_f}, '%Y-%m')
