select *
from (
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct rtk.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'RTK' project
                     FROM suitecrm.jc_meetings_rostelecom rtk
                              left join suitecrm.jc_meetings_rostelecom_cstm rtk_cstm on rtk.id = rtk_cstm.id_c
                     WHERE date(rtk.date_entered) >= '2022-06-01'
                       AND rtk.status != 'Error'
                       and rtk.deleted = 0 ) RTK

         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct bln.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'DR' project
                     FROM suitecrm.jc_meetings_beeline bln
                              left join suitecrm.jc_meetings_beeline_cstm bln_cstm on bln.id = bln_cstm.id_c
                     WHERE date(bln.date_entered) >= '2022-06-01'
                       AND bln.status != 'Error'
                       and bln.deleted = 0)  BLN
         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct dom.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'DR' project
                     FROM suitecrm.jc_meetings_domru dom
                              left join suitecrm.jc_meetings_domru_cstm dom_cstm on id_c = id
                     WHERE date(dom.date_entered) >= '2022-06-01'
                       AND dom.status != 'Error'
                       and dom.deleted = 0)  DOM
         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct ttk.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'DR' project
                     FROM suitecrm.jc_meetings_ttk ttk
                              left join suitecrm.jc_meetings_ttk_cstm ttk_cstm on ttk.id = ttk_cstm.id_c

                     WHERE date(ttk.date_entered) >= '2022-06-01'
                       AND ttk.status != 'Error'
                       and ttk.deleted = 0)  TTK
         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct nbn.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'DR' project
                     FROM suitecrm.jc_meetings_netbynet nbn
                              left join suitecrm.jc_meetings_netbynet_cstm nbn_cstm on nbn.id = nbn_cstm.id_c
                     WHERE date(nbn.date_entered) >= '2022-06-01'
                       AND nbn.status != 'Error'
                       and nbn.deleted = 0) NBN
         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
                        from (SELECT distinct mts.id                                    rtkid,
                                              date(date_entered) date_entered,
                                              'DR' project
                              FROM suitecrm.jc_meetings_mts mts
                                       left join suitecrm.jc_meetings_mts_cstm mts_cstm on mts.id = mts_cstm.id_c
                              WHERE date(mts.date_entered) >= '2022-06-01'
                                AND mts.status != 'Error'
                                and mts.deleted = 0) MTS
         group by 1, 2
         union all
         select date_entered,project,
                count(rtkid) vsego
         from (SELECT distinct mgts.id                                                         rtkid,
                                           date(mgts.date_entered) date_entered,
                                           'DR' project
                           FROM suitecrm.meetings mgts
                                    left join suitecrm.meetings_cstm mgts_cstm on mgts.id = mgts_cstm.id_c
                                    left join (select *
                                               FROM suitecrm.meetings mgts
                                                        left join suitecrm.meetings_cstm mgts_cstm on mgts.id = mgts_cstm.id_c) mgts_cstm_c
                                              ON mgts.id = mgts_cstm_c.id
                           WHERE date(mgts.date_entered) >= '2022-06-01'
                             AND mgts.status != 'Error'
                             and mgts.deleted = 0) MGTS
         group by 1, 2
     ) Meets








