
SELECT dialog AS queue,
       destination_queue,
       date,
       phone
  FROM suitecrm.transferred_to_other_queue
 WHERE DATE_FORMAT(date, '%Y-%m') = DATE_FORMAT({date_f}, '%Y-%m')