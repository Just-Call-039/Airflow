select concat(8, if(length(src) = 11, substring(src, 2, 10), replace(src, '+7', '')))          phone, 
       calldate calldate_cdr, 
       date(calldate)                                                                          call_date, 
       if(second(calldate) > 50 and minute(calldate) = 59, hour(calldate) + 1, hour(calldate)) call_hour, 
       case 
           when second(calldate) > 50 and minute(calldate) = 59 then 0 
           when second(calldate) > 50 then minute(calldate) + 1 
           else minute(calldate) end                                                           call_minute, 

       case when locate('_',channel) != 0 then replace(substring(channel,1,LOCATE('_',channel)-1),'SIP/','') 
           when channel like '%-%' then replace(substring(channel,1,LOCATE('-',channel)-1),'SIP/','') 
           else channel end provider_cdr, 

       did did_cdr, 
       lastapp lastapp_cdr, 
       billsec billsec_cdr, 
       case WHEN disposition = 'NO ANSWER' THEN 0
      WHEN disposition = 'ANSWERED'  THEN 1
      WHEN disposition = 'FAILED'    THEN 2
      WHEN disposition = 'BUSY'      THEN 3
      ELSE 9 end disposition_cdr 
from cdr 
where date(calldate) = date(now()) - interval 1 day
  and dstchannel like '%INBOUND%' 