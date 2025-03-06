select timestamp date_t,
                             case 
                                  when phone = 'anonymous' then '80000000000'
                                  when length(phone) = 10 
                                       then concat('8', phone)
                                  when length(phone) = 11 and phone like '7%%' 
                                       then concat('8', substring(phone, length(phone) - 9))
                                  when length(phone) > 11 and phone not like '+7%%' 
                                       then substring(phone, length(phone) - 10)
                                  else phone
                                  end               phone,
                             if(queue is null, '0', queue) queue_i,
                             exit_point
                             
                        from suitecrm.inbound_calls_info

                       where date(timestamp) = '{}'