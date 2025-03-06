with robotlog as (select phone,
                         city_c,
                         date(call_date)                       datecall,
                         substring(dialog, 11, 4)              set_queue,
                         town,
                         last_step,
                         uniqueid
                  from suitecrm_robot.jc_robot_log
                  where last_step not in ('', '0', '1', '261', '262', '111', '361', '362', '371', '372')
                    and month(call_date) = month(curdate() - interval {n} month)
                    and year(call_date) = if(month(curdate() - interval {n} month) = 12, year(curdate() - interval 1 year), year(curdate()))),
                   

     reportcongif as (select substring(turn, 11, 4) dialog, steps_transferred
                      from jc_robot_reportconfig),

     tabeue as (select dialog,
                       steps_transferred,
                       SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 1), ',', -1) as queue_1,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 2), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 1), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 2), ',', -1)
                           end                                                              as queue_2,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 3), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 2), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 3), ',', -1)
                           end                                                              as queue_3,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 4), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 3), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 4), ',', -1)
                           end                                                              as queue_4,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 5), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 4), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 5), ',', -1)
                           end                                                              as queue_5,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 6), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 5), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 6), ',', -1)
                           end                                                              as queue_6,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 7), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 6), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 7), ',', -1)
                           end                                                              as queue_7,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 8), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 7), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 8), ',', -1)
                           end                                                              as queue_8,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 9), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 8), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 9), ',', -1)
                           end                                                              as queue_9,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 10), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 9), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 10), ',', -1)
                           end                                                              as queue_10,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 11), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 10), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 11), ',', -1)
                           end                                                              as queue_11,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 12), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 11), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 12), ',', -1)
                           end                                                              as queue_12,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 13), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 12), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 13), ',', -1)
                           end                                                              as queue_13,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 14), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 13), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 14), ',', -1)
                           end                                                              as queue_14,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 15), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 14), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 15), ',', -1)
                           end                                                              as queue_15,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 16), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 15), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 16), ',', -1)
                           end                                                              as queue_16,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 17), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 16), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 17), ',', -1)
                           end                                                              as queue_17,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 18), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 17), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 18), ',', -1)
                           end                                                              as queue_18,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 19), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 18), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 19), ',', -1)
                           end                                                              as queue_19,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 20), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 19), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 20), ',', -1)
                           end                                                              as queue_20,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 21), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 21), ',', -1)
                           end                                                              as queue_21,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 22), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 21), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 22), ',', -1)
                           end                                                              as queue_22,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 23), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 22), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 23), ',', -1)
                           end                                                              as queue_23,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 24), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 23), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 24), ',', -1)
                           end                                                              as queue_24,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 25), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 24), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 25), ',', -1)
                           end                                                              as queue_25,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 26), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 25), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 26), ',', -1)
                           end                                                              as queue_26,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 27), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 26), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 27), ',', -1)
                           end                                                              as queue_27,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 28), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 27), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 28), ',', -1)
                           end                                                              as queue_28,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 29), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 28), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 29), ',', -1)
                           end                                                              as queue_29,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 30), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 29), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 30), ',', -1)
                           end                                                              as queue_30,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 31), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 31), ',', -1)
                           end                                                              as queue_31,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 32), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 31), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 32), ',', -1)
                           end                                                              as queue_32,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 33), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 32), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 33), ',', -1)
                           end                                                              as queue_33,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 34), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 33), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 34), ',', -1)
                           end                                                              as queue_34,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 35), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 34), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 35), ',', -1)
                           end                                                              as queue_35,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 36), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 35), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 36), ',', -1)
                           end                                                              as queue_36,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 37), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 36), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 37), ',', -1)
                           end                                                              as queue_37,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 38), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 37), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 38), ',', -1)
                           end                                                              as queue_38,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 39), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 38), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 39), ',', -1)
                           end                                                              as queue_39,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 40), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 39), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(steps_transferred, ',', 40), ',', -1)
                           end                                                              as queue_40
                from reportcongif
                where steps_transferred is not null
                  and steps_transferred != ''),

     steps as (select *
               from (
                        select dialog,
                               queue_1 as laststep
                        from tabeue
                        union all
                        select dialog, queue_2
                        from tabeue
                        union all
                        select dialog, queue_3
                        from tabeue
                        union all
                        select dialog, queue_4
                        from tabeue
                        union all
                        select dialog, queue_5
                        from tabeue
                        union all
                        select dialog, queue_6
                        from tabeue
                        union all
                        select dialog, queue_7
                        from tabeue
                        union all
                        select dialog, queue_8
                        from tabeue
                        union all
                        select dialog, queue_9
                        from tabeue
                        union all
                        select dialog, queue_10
                        from tabeue
                        union all
                        select dialog, queue_11
                        from tabeue
                        union all
                        select dialog, queue_12
                        from tabeue
                        union all
                        select dialog, queue_13
                        from tabeue
                        union all
                        select dialog, queue_14
                        from tabeue
                        union all
                        select dialog, queue_15
                        from tabeue
                        union all
                        select dialog, queue_16
                        from tabeue
                        union all
                        select dialog, queue_17
                        from tabeue
                        union all
                        select dialog, queue_18
                        from tabeue
                        union all
                        select dialog, queue_19
                        from tabeue
                        union all
                        select dialog, queue_20
                        from tabeue
                        union all
                        select dialog, queue_21
                        from tabeue
                        union all
                        select dialog, queue_22
                        from tabeue
                        union all
                        select dialog, queue_23
                        from tabeue
                        union all
                        select dialog, queue_24
                        from tabeue
                        union all
                        select dialog, queue_25
                        from tabeue
                        union all
                        select dialog, queue_26
                        from tabeue
                        union all
                        select dialog, queue_27
                        from tabeue
                        union all
                        select dialog, queue_28
                        from tabeue
                        union all
                        select dialog, queue_29
                        from tabeue
                        union all
                        select dialog, queue_30
                        from tabeue
                        union all
                        select dialog, queue_31
                        from tabeue
                        union all
                        select dialog, queue_32
                        from tabeue
                        union all
                        select dialog, queue_33
                        from tabeue
                        union all
                        select dialog, queue_34
                        from tabeue
                        union all
                        select dialog, queue_35
                        from tabeue
                        union all
                        select dialog, queue_36
                        from tabeue
                        union all
                        select dialog, queue_37
                        from tabeue
                        union all
                        select dialog, queue_38
                        from tabeue
                        union all
                        select dialog, queue_39
                        from tabeue
                        union all
                        select dialog, queue_40
                        from tabeue
                    ) as t2
               where laststep is not null),

     robotconfig as (select phone, city_c, datecall, robotlog.set_queue queue, town, uniqueid
                     from robotlog
                              left join steps
                                        on (robotlog.set_queue = steps.dialog and last_step = laststep)),

     address as (select distinct  phone,
                                 calldate dateadress,
                                 substring(dialog, 11, 4)              set_queue,
                                 city
                 from address_log
                 where month(calldate) = month(curdate() - interval {n} month)
                   and year(calldate) = if(month(curdate() - interval {n} month) = 12, year(curdate() - interval 1 year), year(curdate()))
                   and city is not null),
                   

     transferLog as (select distinct t.phone,
                                     date(date)            datecalls,
                                     city_c,
                                     city,
                                     town,
                                     t.dialog,
                                     destination_queue,
                                     DATE_FORMAT(DATE_ADD(date, INTERVAL IF(MINUTE(date) >= 58, 1, 0) HOUR),
                                                 '%H') + 0 hoursonly
                     from transferred_to_other_queue t
                              left join robotconfig r
                                        on (t.phone = r.phone and date(r.datecall) = date(date) and
                                            dialog = queue and t.uniqueid = r.uniqueid)
                              left join address on (address.phone = t.phone and
                                                    date(date) = date(dateadress) and
                                                    dialog = set_queue)
                     where month(date) = month(curdate() - interval {n} month)
                       and year(date) = if(month(curdate() - interval {n} month) = 12, year(curdate() - interval 1 year), year(curdate())))

select if(length(replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''),
                         ' ',
                         '')) <=
          10,
          concat(8,
                 replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''),
                         ' ', '')),
          concat(8,
                 right(replace(
                               replace(replace(replace(phone, '-', ''), ')', ''), '(', ''),
                               ' ',
                               ''), 10))) as phone,
       datecalls,
       city_c,
       city,
       town,
       dialog,
       destination_queue,
       hoursonly
from transferLog








