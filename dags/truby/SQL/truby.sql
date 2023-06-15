select distinct date(calldate)            call_date,
    hour(calldate)            hour,
    minute(calldate)            minute,
                 clid,
                 disposition,
                 substring(channel, 5, 13) way,
           substring(channel, 16, 2) gate
          from asteriskcdrdb.cdr
          where date(calldate) = date(now()) - interval {} day