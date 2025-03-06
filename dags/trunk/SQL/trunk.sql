select 
       calldate,
       src,
       replaceOne(outbound_cnum, '+', '') as trunk_number,
       dstchannel
  from asteriskcdrdb_all.cdr
 where toDate(calldate) = toDate(now())
   and gateway != '1'
   and dstchannel not like '%INBOUND%'