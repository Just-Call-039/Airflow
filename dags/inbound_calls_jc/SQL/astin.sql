select if(substr(src, 1, 1) = '7', concat('8', substr(src, 2, 10)), src)                       phone, 
       start calldate_astin, 
       toDate(start)                                                                           call_date, 
       if(toSecond(start) > 50 and toMinute(start) = 59, toHour(start) + 4, toHour(start) + 3) call_hour, 
       case 
           when toSecond(start) > 50 and toMinute(start) = 59 then 0 
           when toSecond(start) > 50 then toMinute(start) + 1 
           else toMinute(start) end                                                            call_minute, 
       dcontext dcontext_astin, 
       lastapp lastapp_astin, 
       case when dstchannel like '%JUSTCALL%' then 'to_RO' 
           when  dstchannel like '%A1_A0%' then 'to_AM' 
               else lastdata end shoulder_astin, 
       disposition disposition_astin, 1 as disposition_cdr 
from asteriskcdrdb_all.astin_cdr 

where toDate(start) = toDate(now()) - interval 1 day  