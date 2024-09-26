SELECT dialog AS queue,
       destination_queue,
       date,
       phone
  FROM suitecrm.transferred_to_other_queue
 WHERE date(date) = date(now())
