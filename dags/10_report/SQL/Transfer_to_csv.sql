select *
from (select 'dialog', 'destination_queue', 'date', 'uniqueid', 'phone'
      union all
      select *
      from suitecrm.transferred_to_other_queue
      where date(date) between date(now()) - interval 65 day and date(now())) as t1
into outfile '/var/lib/mysql-files/10_report/transfer.csv'
    fields terminated by ';';
