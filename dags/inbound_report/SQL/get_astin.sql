 select 
        userfield,
        case
            when src = 'anonymous' then '80000000000'
            when length(src) > 11 and src not like '+7%%' then substring(src, length(src) - 10)
            when length(src) = 10 and position(src, '9') = 1 then concat('8', src)
            when position(src, '7') = 1 then replaceOne(src, '7', '8')
            when position(src, '+7') = 1 then replaceOne(src, '+7', '8')
            else src
        end                       phone,
        date_add(HOUR, 3, start)  date_a,
        billsec                   billsec_a,
        if(dstchannel like '%%JUSTCALL%%', 'robot',
            if(dstchannel like '%%A0%%', 'operator', ''))  active,
            lastapp                lastapp_a

  from asteriskcdrdb_all.astin_cdr
 where start BETWEEN '{} 21:00:00' AND '{} 21:00:00'