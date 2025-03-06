     select
            calldate          date_t,
            did,
            CASE 
                  WHEN src = 'anonymous' THEN '80000000000'
                  WHEN length(src) > 11 and src not like '+7%%' THEN substring(src, length(src) - 10)
                  WHEN length(src) = 10 and src like '9%%' THEN concat('8', src)
                  WHEN (src like '+7%%') OR (src like '7%%') THEN concat('8', substring(src, length(src) - 9))
                  ELSE src 
            END               phone,
            uniqueid,
            CASE 
                  WHEN did = '4493720' THEN 'gaz'
                  WHEN did in ('0381213', '0390609','0390926', '0392455', '0392894', '0394463', 
                              '0394470', '0402426', '0412837', '0412840') THEN '101_internet'
                  WHEN did = '20123' OR did = '20124' THEN 'tarifnik'
                  WHEN lastdata =  "SIP/INBOUND_CALLS_ASTIN/7777777" THEN 'rtk'
                  ELSE '0'  
                  END              project,
            billsec           billsec_t,
            lastapp           lastapp_t

       from asteriskcdrdb.cdr
      where date(calldate) BETWEEN '{}' AND '{}'
            and dstchannel like '%%INBOUND%%'