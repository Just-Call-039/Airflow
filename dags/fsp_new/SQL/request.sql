with ocheredi as (select distinct *
                  from (select queue,
                               project,
                               date,
                               (row_number() over (partition by queue order by date desc)) as rw
                        from suitecrm.queue_project
                        where date >= '2022-02-01') as tb1
                  where rw = 1
                  order by 3),
     department as (select '12' team, 'Входящая линия' department
                    union all
                    select '50' team, 'Диспетчера Алексеевой' department
                    union all
                    select '28' team, 'Универсалы' department
                    union all
                    select '4' team, 'Диспетчера Кротченко' department
                    union all
                    select '42' team, 'Авито' department
                    union all
                    select '16' team, 'Банкроты' department),
     teams as (select *,
                        REGEXP_SUBSTR(first_name, '[0-9]+') team

               from (
                        SELECT id,
                               concat(first_name, ' ', last_name) fio,
                               case
                                   when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
                                       then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
                                   when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
                                       then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
                                   else first_name
                                   end                            first_name
                        FROM suitecrm.users) user),
     operatory as (select '71eea5ba-9c5a-b7f8-b457-64f82d25a7a2' as id_user, 'MTS' project_before, 'GULFSTREAM' project_after union all
                    select '961a8d10-c1b8-3c4f-5a4b-5d7756de6385' as id_user, 'MTS' project_before, 'GULFSTREAM' project_after union all
                    select '25e10c1d-4cec-995f-b3a2-6537708ed333' as id_user, 'MTS' project_before, 'GULFSTREAM' project_after union all
                    select 'c75f419e-b249-4b48-20cf-65ca382be50a' as id_user, 'MTS' project_before, 'GULFSTREAM' project_after union all
                    select '2fc65ac0-c6c4-4bb2-a1f1-62fb5c35fbf4' as id_user, 'MTS' project_before, 'GULFSTREAM' project_after union all
                    select '4e2bd871-2d7d-5470-3a84-64f6ebf233f1' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '809fd8e2-9d83-a61d-dc43-626f8804e165' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '31247f69-833c-704c-7c35-6172d27d413c' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select 'bf59e8fe-091a-42bf-c69b-655c7415970b' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '1b79bed3-7f1c-b0e4-03ed-64f6ec5d2e00' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '663b38f7-2f87-145e-c0ec-62a84b42b409' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '107ab6e2-3c2f-392e-c1d5-60b8bbe0f455' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '653ba48b-98a7-1661-c459-63624e819223' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '4411e813-2e3b-f4fe-94aa-5c9b8103af67' as id_user, 'MTS' project_before, 'NBN' project_after union all
                    select '8a3a332c-ffbc-ce31-112c-61923e6a9f1d' as id_user, 'MTS' project_before, 'NBN' project_after      ),
# clear_users as (select id,
#                             concat(first_name, ' ', last_name) fio,
#                             first_name,
#                             case
#                                 when substring_index(substring_index(first_name, ' ', 3), ' ', -1) REGEXP '^[0-9]+$'
#                                     then substring_index(substring_index(first_name, ' ', 3), ' ', -1)
#                                 when substring_index(substring_index(first_name, ' ', 4), ' ', -1) REGEXP '^[0-9]+$'
#                                     then substring_index(substring_index(first_name, ' ', 4), ' ', -1)
#                                 else
#                                     (case
#                                          when left(first_name, instr(first_name, ' ') - 1) > 0 and
#                                               left(first_name, instr(first_name, ' ') - 1) < 10000
#                                              then left(first_name, instr(first_name, ' ') - 1)
#                                          when left(first_name, 2) = 'я_'
#                                              then substring(first_name, 3, (instr(first_name, ' ') - 3))
#                                          when left(first_name, 1) = 'я'
#                                              then substring(first_name, 2, (instr(first_name, ' ') - 1))
#                                          else '' end)
#                                 end                            teams
#                      from suitecrm.users),
#      supervisors as (select id as super, fio as super_fio, replace(teams, ' ', '') team
#                      from clear_users
#                      where id in (select distinct supervisor
#                                   from suitecrm.worktime_supervisor)),
#      teams as (select clear_users.id, fio, date(date_start) start, if(date_stop is null, date(now()), date(date_stop)-interval 1 day) stop, super as supervisor, supervisors.team
#          from clear_users
#          left join suitecrm.worktime_supervisor on clear_users.id = id_user
#          left join supervisors on super = supervisor),
     contacts as (select phone_work,
                         if(city_c is null or city_c = '', concat(contacts_cstm.town_c, '_t'), city_c) as city,
                         town_c,
                         city_c,
                         case when base_source_c like '%301%' then '55_m'
when base_source_c like '%302%' then '18_m'
when base_source_c like '%303%' then '55_f'
when base_source_c like '%304%' then '18_f' else '' end department
                  from suitecrm.contacts
                           left join suitecrm.contacts_cstm on id = id_c)

select Meets.*
#        ,case
#            when last_queue_c = 9251 then 'Just Job'
#            when department is null and locate('LIDS', proect) > 0 then 'Лиды'
#            when department is null then 'КЦ'
#            else department end department
from (
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                active_date,
                city_c,
#                 if(date_entered < '2023-04-01', city_c, city) city_c,
                town_c,
                if(proect in ('RTK LIDS','RTK'), proect, 'RTK LIDS') as module
#                 ,
#                                      start,
#                                      stop
         , department
         from (select R.*,
                      case
                          when team = 28 then 'TELE2'
                          when team in (12, 50, 4) then 'RTK'
                          when team in (19, 42, 80, 107, 13, 555, 123) then 'RTK'
                          when team = 13 then 'RTK'
                          when team in
                               (1, 2, 3, 5, 11, 11, 11, 11, 16, 21, 26, 26, 29, 31, 35, 37, 38, 43, 47, 48, 51, 52, 54,
                                59, 60, 66, 67, 68, 69, 70, 71, 78, 79,
                                83, 84, 84, 84, 85, 86, 88, 89, 92, 93, 94, 95, 96, 97, 98, 99, 105, 106, 109, 113, 115,
                                116, 119, 120, 666, 124) then 'RTK LIDS'
                          else 'RTK LIDS'
                          end proect
               from (SELECT distinct rtk.id                       rtkid,
#                                      first_name,
                                     rtk_cstm.last_queue_c,
                                     team,
                                     date(activate_date_internet) active_date,
                                     teams.id                     uid,
                                     fio,
                                     date(rtk.date_entered) as    date_entered,
                                     rtk.status,
                                     case
                                         when rtk.status in ('Held', 'Active') then 1
                                         when rtk.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                         else 0
                                         end                      konva,
                                     packet_service_c             tarif,
                                     rtk.phone_work,
                                     city_c,
                                     city,
                                     town_c
#                                      ,
#                                      start,
#                                      stop
               , department
                     FROM suitecrm.jc_meetings_rostelecom rtk
                              left join suitecrm.jc_meetings_rostelecom_cstm rtk_cstm on rtk.id = rtk_cstm.id_c
                              left join teams on rtk.assigned_user_id = teams.id
                              left join contacts on rtk.phone_work = contacts.phone_work
                     WHERE date(rtk.date_entered) >= '2024-04-01'
                       AND rtk.status not in ('Error', 'doubled')
                       and rtk.deleted = 0) R
              ) RTK
         union all
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                '' active_date,
                city_c,
#                 if(date_entered < '2023-04-01', city_c, city) city_c,
                town_c,
                if(proect in ('BEELINE LIDS','BEELINE'), proect, 'BEELINE LIDS') as module
#                 ,
#                                      start,
#                                      stop
         , department
         from (select R.*,
                      case
                          when team in (12, 50, 4) then 'BEELINE'
                          when team in (13, 10, 15, 8, 27, 28, 555) then 'BEELINE'
                          when team in (122, 667, 432) then 'BEELINE LIDS'
                          else 'BEELINE'
                          end proect
               from (SELECT distinct bln.id                    rtkid,
#                                      first_name,
                                     bln_cstm.last_queue_c,
                                     teams.team,
                                     teams.id                  uid,
                                     fio,
                                     date(bln.date_entered) as date_entered,
                                     bln.status,
                                     case
                                         when bln.status in ('Held', 'Active', 'Proverka', 'delivered') then 1
                                         when bln.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                         else 0
                                         end                   konva,
                                     case
                                         when packet_service in ('mono_shpd', 'shpd_tv') then 'shpd'
                                         when type_c = 'Beeline3' then 'konvergent'
                                         else 'shpd' end       tarif,
                                     bln.phone_work,
                                     city_c,
                                     city,
                                     contacts.town_c

               , department
                     FROM suitecrm.jc_meetings_beeline bln
                              left join suitecrm.jc_meetings_beeline_cstm bln_cstm on bln.id = bln_cstm.id_c
                              left join teams on bln.assigned_user_id = teams.id
                              left join contacts on bln.phone_work = contacts.phone_work
                     WHERE date(bln.date_entered) >= '2024-04-01'
                       AND bln.status != 'Error'
                       and bln.deleted = 0) R) BLN
         union all
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                '' active_date,
                city_c,

                town_c,
                if(proect in ('DOMRU LIDS','DOMRU'), proect, 'DOMRU LIDS') as module

         , department
         from (select distinct R.*,
                               case
                                   when packet_service in ('tel', 'tel_RB', 'tel_R', 'tel_B') then 'DOMRU Dop'
                                   when team in (19, 42, 80, 107, 13, 9) then 'DOMRU'
                                   when team in (12, 50, 4) then 'DOMRU'
                                   when team in
                                        (20, 24, 40, 62, 63, 72, 90, 100, 500, 501, 502, 503, 504, 505, 506, 507, 508)
                                       then 'DOMRU LIDS'
                                   else 'DOMRU LIDS'
                                   end                     proect,
                               case
                                   when router = 1 and tv_box = 1 then concat(packet_service, '_RB')
                                   when router = 1 then concat(packet_service, '_R')
                                   when tv_box = 1 then concat(packet_service, '_B')
                                   else packet_service end tarif
               from (SELECT distinct dom.id                                     rtkid,

                                     dom.last_queue_c,
                                     team,
                                     teams.id                                   uid,
                                     fio,
                                     date(dom.date_entered) as                  date_entered,
                                     dom.status,
                                     case
                                         when dom.status in ('Held', 'Active') then 1
                                         when dom.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                         else 0
                                         end                                    konva,
                                     if(router_c in ('in_kredit', 'buy'), 1, 0) router,
                                     if(tv_box_c = 1, 1, 0)                     tv_box,
                                     case
                                         when packet_service = 'home_phone' then 'tel'
                                         when packet_service = 'internet' then 'int'
                                         when packet_service = 'tv' then 'tv'
                                         when packet_service = 'ktv' then 'ktv'
                                         when packet_service = 'int_m' then 'int_ktv'
                                         when packet_service = 'int_s' then 'int_iktv'
                                         else '' end                            packet_service,
                                     dom.phone_work,
                                     city_c,
                                     city,
                                     town_c

, department
                     FROM suitecrm.jc_meetings_domru dom
                              left join suitecrm.jc_meetings_domru_cstm dom_cstm on id_c = id
                              left join teams on dom.assigned_user_id = teams.id
                              left join contacts on dom.phone_work = contacts.phone_work
                     WHERE date(dom.date_entered) >= '2024-04-01'
                       AND dom.status != 'Error'
                       and dom.deleted = 0) R) DOM
         union all
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                '' active_date,
                city_c,

                town_c,
                if(proect in ('TTK LIDS','TTK'), proect, 'TTK LIDS') as module

         , department
         from (select R.*,
                      case
                          when team in (19, 42, 80, 107, 13) then 'TTK'
                          when team in (12, 50, 4) then 'TTK'
                          when team in (14, 17, 33, 34, 36, 39, 41, 44, 61, 118, 121) then 'TTK LIDS'
                          else 'TTK LIDS'
                          end proect
               from (SELECT distinct ttk.id                    rtkid,

                                     ttk_cstm.last_queue_c,
                                     team,
                                     teams.id                  uid,
                                     fio,
                                     date(ttk.date_entered) as date_entered,
                                     ttk.status,
                                     case
                                         when ttk.status in ('Held', 'Active') then 1
                                         when ttk.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                         else 0
                                         end                   konva,
                                     check_internet,
                                     check_tv,
                                     case
                                         when check_internet = 1 and check_tv = 1 then 'packet'
                                         when check_internet = 1 then 'shpd'
                                         when check_tv = 1 then 'tv'
                                         when type_internet_c is not null and type_tv_c is not null then 'packet'
                                         when type_internet_c is not null then 'shpd'
                                         when type_tv_c is not null then 'tv'
                                         else 0
                                         end                   tarif,
                                     ttk.phone_work,
                                     city_c,
                                     city,
                                     town_c

               , department
                     FROM suitecrm.jc_meetings_ttk ttk
                              left join suitecrm.jc_meetings_ttk_cstm ttk_cstm on ttk.id = ttk_cstm.id_c
                              left join teams on ttk.assigned_user_id = teams.id
                              left join contacts
                                        on ttk.phone_work = contacts.phone_work
                     WHERE date(ttk.date_entered) >= '2024-04-01'
                       AND ttk.status != 'Error'
                       and ttk.deleted = 0) R) TTK
         union all
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                '' active_date,
                city_c,

                town_c,
                if(proect in ('NBN LIDS','NBN'), proect, 'NBN LIDS') as module

         , department
         from (select R.*,
                      case
                          when id_user is not null then project_after
                          when team in (12, 50, 4) then 'NBN'
                          when team in (19, 42, 80, 107, 13, 8, 32) then 'NBN'
                          when team in (7, 22, 45, 46, 49, 53) then 'NBN LIDS'
                          else 'NBN LIDS' end proect
               from (SELECT distinct nbn.id                    rtkid,
                                     nbn_cstm.last_queue_c,
                                     team,
                                     teams.id                  uid,
                                     fio,
                                     date(nbn.date_entered) as date_entered,
                                     nbn.status,
                                     case
                                         when nbn.status in ('Held', 'Active') then 1
                                         when nbn.status in ('Created', 'provider_planned', 'dispetcher_grafik') then 2
                                         else 0
                                         end                   konva,
                                     case
                                         when check_internet + check_tv + check_mobile >= 2 then 'konvergent'
                                         when check_internet + check_tv = 1 then 'shpd'
                                         else 'ne_vybran' end              tarif,
                                     nbn.phone_work,
                                     city_c,
                                     city,
                                     contacts.town_c

               , department
                     FROM suitecrm.jc_meetings_netbynet nbn
                              left join suitecrm.jc_meetings_netbynet_cstm nbn_cstm on nbn.id = nbn_cstm.id_c
                              left join teams on nbn.assigned_user_id = teams.id
                              left join contacts on nbn.phone_work = contacts.phone_work
                     WHERE date(nbn.date_entered) >= '2024-04-01'
                       AND nbn.status != 'Error'
                       and nbn.deleted = 0) R
                left join operatory on uid = id_user
               where last_queue_c is null
                  or last_queue_c != 9134) NBN
         union all
         select last_queue_c,
                proect,
                R2.team as team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                ''         active_date,
                city_c,
                town_c,
                case when proect in ('MTS LIDS','MTS','GULFSTREAM Job') then proect
                     when proect in ('GULFSTREAM LIDS','GULFSTREAM') and tarif = 'Satellite_TV' then proect
                    else 'MTS LIDS' end  module

         , department
         from (select rtkid,
                      last_queue_c,
                      tmts.team,
                      uid,
                      fio,
                      date_entered,
                      status,
                      konva,
                      phone_work,
                      city_c,
                      city,
                      town_c,
                      case
                          when id_user is not null then project_before
                          when last_queue_c = 9251 then 'GULFSTREAM Job'
                          when team = 16 then 'GULFSTREAM'
                          when team = 17 then 'GULFSTREAM LIDS'
                          when team in (19, 42, 80, 107, 13, 123, 15, 25, 27, 28, 30) then 'MTS'
                          when team in (12, 50, 4) then 'MTS'
                          when team in
                               (6, 18, 23, 55, 56, 57, 58, 64, 65, 73, 74, 76, 77, 81, 87, 91, 102, 103, 104, 117, 122,
                                202) then 'MTS LIDS'
                          else 'MTS LIDS' end proect,
                      tarif

         , department
               from (SELECT distinct mts.id                    rtkid,
                                     mts_cstm.last_queue_c,
                                     teams.team,
                                     teams.id                  uid,
                                     fio,
                                     date(mts.date_entered) as date_entered,
                                     mts.status                status,
                                     case
                                         when mts.status in ('Held', 'Active') then 1
                                         when mts.status in ('Created', 'provider_planned', 'dispetcher_grafik')
                                             then 2
                                         else 0
                                         end                   konva,
                                     packet_service_c          tarif,
                                     mts.phone_work,
                                     city_c,
                                     city,
                                     town_c

               , department
                     FROM suitecrm.jc_meetings_mts mts
                              left join suitecrm.jc_meetings_mts_cstm mts_cstm on mts.id = mts_cstm.id_c
                              left join ocheredi on mts_cstm.last_queue_c = ocheredi.queue
                              left join teams on mts.assigned_user_id = teams.id
                              left join contacts on mts.phone_work = contacts.phone_work
                     WHERE date(mts.date_entered) >= '2024-04-01'
                       AND mts.status != 'Error'
                       and mts.deleted = 0) tmts
             left join operatory on uid = id_user

             ) R2
         union all
         select last_queue_c,
                proect,
                team,
                uid,
                fio,
                date_entered,
                status,
                konva,
                tarif,
                rtkid,
                phone_work,
                active_date,
                city_c,
#                 if(date_entered < '2023-04-01', city_c, city) city_c,
                town_c,
                if(proect in ('GULFSTREAM LIDS',

                'GULFSTREAM', 'TELE2', 'Delta', 'Цезарь LIDS'), proect, 'DR') as module

#                 ,
#                                      start,
#                                      stop
         , department
         from (select R.*,
                      case
                          when id_user is not null then project_after

                          when sys_project = 'project_8' and team in (30,13,159,8) then 'GULFSTREAM'
                          when sys_project = 'project_8' and team in (17,15) then 'GULFSTREAM LIDS'
                          when sys_project = 'tele2' then 'TELE2'
                          when sys_project = 'project_2' then 'VSK LIDS'
                          when sys_project = 'project_5' then 'Цезарь LIDS'
                          when sys_project = 'project_4' then 'Delta'
                          when sys_project = 'project_1' then 'TAT LIDS'

                          else 'DR Job'
                          end proect
               from (SELECT distinct other.id                     rtkid,

                                     other.last_queue last_queue_c,
                                     team,
                                     date(activate_date_internet) active_date,
                                     teams.id                     uid,
                                     fio,
                                     date(other.date_entered) as  date_entered,
                                     other.status,
                                     case
                                         when other.status in ('Held', 'Active') then 1
                                         when other.status in ('Created', 'provider_planned', 'dispetcher_grafik')
                                             then 2
                                         else 0
                                         end                      konva,
                                     case
                                         when check_internet + check_tv + check_mobile >= 3 then 'konvergent'
                                         when check_internet + check_mobile >= 2 then 'packet_sim'
                                         when check_internet + check_tv >= 2 then 'packet_tv'
                                         when check_internet = 1 and check_mobile = 0 then 'int'
                                         when check_mobile = 1 then 'sim'
                                         else 'ne_vybran' end              tarif,
                                     other.phone_work,
                                     city_c,
                                     city,
                                     providers,
                                     other.project                sys_project,
                                     rtk_cstm.selection_project_c,
                                     town_c

               , department
                     FROM suitecrm.jc_meetings_other other
                              left join suitecrm.jc_meetings_other_cstm rtk_cstm on other.id = rtk_cstm.id_c
                              left join teams on other.assigned_user_id = teams.id
                              left join contacts on other.phone_work = contacts.phone_work
                     WHERE date(other.date_entered) >= '2024-04-01'
                       AND other.status not in ('Error', 'doubled')
                       and other.deleted = 0) R
             left join operatory on uid = id_user
              ) OTHER
     ) Meets
