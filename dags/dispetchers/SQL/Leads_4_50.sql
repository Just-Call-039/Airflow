with town_c as (select 0 town_c, '0 РФ' Город
                union all
                select 1, '1 Адыгея'
                union all
                select 2, '2 Башкортостан'
                union all
                select 3, '3 Бурятия'
                union all
                select 4, '4 Алтай'
                union all
                select 5, '5 Дагестан'
                union all
                select 6, '6 Ингушетия'
                union all
                select 7, '7 Кабардино-Балкарская'
                union all
                select 8, '8 Калмыкия'
                union all
                select 9, '9 Карачаево-Черкесская'
                union all
                select 10, '10 Карелия'
                union all
                select 11, '11 Коми'
                union all
                select 12, '12 Марий Эл'
                union all
                select 13, '13 Мордовия'
                union all
                select 14, '14 Саха /Якутия/'
                union all
                select 15, '15 Северная Осетия - Алания'
                union all
                select 16, '16 Татарстан'
                union all
                select 17, '17 Тыва'
                union all
                select 18, '18 Удмуртская'
                union all
                select 19, '19 Хакасия'
                union all
                select 20, '20 Чеченская'
                union all
                select 21, '21 Чувашская'
                union all
                select 22, '22 Алтайский'
                union all
                select 23, '23 Краснодарский'
                union all
                select 24, '24 Красноярский'
                union all
                select 25, '25 Приморский'
                union all
                select 26, '26 Ставропольский'
                union all
                select 27, '27 Хабаровский'
                union all
                select 28, '28 Амурская'
                union all
                select 29, '29 Архангельская'
                union all
                select 30, '30 Астраханская'
                union all
                select 31, '31 Белгородская'
                union all
                select 32, '32 Брянская'
                union all
                select 33, '33 Владимирская'
                union all
                select 34, '34 Волгоградская'
                union all
                select 35, '35 Вологодская'
                union all
                select 36, '36 Воронежская'
                union all
                select 37, '37 Ивановская'
                union all
                select 38, '38 Иркутская'
                union all
                select 39, '39 Калининградская'
                union all
                select 40, '40 Калужская'
                union all
                select 41, '41 Камчатский'
                union all
                select 42, '42 Кемеровская область - Кузбасс'
                union all
                select 43, '43 Кировская'
                union all
                select 44, '44 Костромская'
                union all
                select 45, '45 Курганская'
                union all
                select 46, '46 Курская'
                union all
                select 47, '47 Ленинградская'
                union all
                select 48, '48 Липецкая'
                union all
                select 49, '49 Магаданская'
                union all
                select 50, '50 Московская'
                union all
                select 51, '51 Мурманская'
                union all
                select 52, '52 Нижегородская'
                union all
                select 53, '53 Новгородская'
                union all
                select 54, '54 Новосибирская'
                union all
                select 55, '55 Омская'
                union all
                select 56, '56 Оренбургская'
                union all
                select 57, '57 Орловская'
                union all
                select 58, '58 Пензенская'
                union all
                select 59, '59 Пермский'
                union all
                select 60, '60 Псковская'
                union all
                select 61, '61 Ростовская'
                union all
                select 62, '62 Рязанская'
                union all
                select 63, '63 Самарская'
                union all
                select 64, '64 Саратовская'
                union all
                select 65, '65 Сахалинская'
                union all
                select 66, '66 Свердловская'
                union all
                select 67, '67 Смоленская'
                union all
                select 68, '68 Тамбовская'
                union all
                select 69, '69 Тверская'
                union all
                select 70, '70 Томская'
                union all
                select 71, '71 Тульская'
                union all
                select 72, '72 Тюменская'
                union all
                select 73, '73 Ульяновская'
                union all
                select 74, '74 Челябинская'
                union all
                select 75, '75 Забайкальский'
                union all
                select 76, '76 Ярославская'
                union all
                select 77, '77 Москва'
                union all
                select 78, '78 Санкт-Петербург'
                union all
                select 79, '79 Еврейская'
                union all
                select 83, '83 Ненецкий'
                union all
                select 86, '86 Ханты-Мансийский Автономный округ - Югра'
                union all
                select 87, '87 Чукотский'
                union all
                select 89, '89 Ямало-Ненецкий'
                union all
                select 91, '91 Крым'
                union all
                select 92, '92 Севастополь'),
     BP as (select phone phone_bp, date_start date_start_bp
            from suitecrm.jc_planned_calls jpc
            where date(date_start) > date(now()) - interval 1 day),
     Meets_90 as (SELECT phone_work
                  from (SELECT rtk.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_rostelecom rtk
                                 left join suitecrm.jc_meetings_rostelecom_cstm rtk_cstm on rtk.id = rtk_cstm.id_c
                        union all
                        SELECT bln.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_beeline bln
                                 left join suitecrm.jc_meetings_beeline_cstm bln_cstm on bln.id = bln_cstm.id_c
                        union all
                        SELECT dom.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_domru dom
                        union all
                        SELECT ttk.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_ttk ttk
                                 left join suitecrm.jc_meetings_ttk_cstm ttk_cstm on ttk.id = ttk_cstm.id_c
                        union all
                        SELECT nbn.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_netbynet nbn
                                 left join suitecrm.jc_meetings_netbynet_cstm nbn_cstm on nbn.id = nbn_cstm.id_c
                        union all
                        SELECT mts.phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_mts mts
                                 left join suitecrm.jc_meetings_mts_cstm mts_cstm on mts.id = mts_cstm.id_c
                        union all
                        select phone_work phone_work, date_entered, status, deleted
                        FROM suitecrm.jc_meetings_beeline_mnp sim
                        union all
                        SELECT mgts_cstm.client_mob_c phone_work, date_entered, status, deleted
                        FROM suitecrm.meetings mgts
                                 left join suitecrm.meetings_cstm mgts_cstm on mgts.id = mgts_cstm.id_c) Created_Meets
                  where deleted = 0
                    and status != 'Error'
                    and date(date_entered) between (NOW() - INTERVAL 90 day) and NOW()),
     otkaz_c as (select 'otkaz23' otkaz_c, 'Нет тех. возможности' Причина_отказа
                 union all
                 select 'otkaz_24', 'НЕГАТИВНЫЙ КЛИЕНТ'
                 union all
                 select 'otkaz_10', 'Не ЛПР'
                 union all
                 select 'otkaz_8', 'Услуга уже подключена'
                 union all
                 select 'otkaz_5', 'Отказ без объяснения'
                 union all
                 select 'covid', 'Перезвонить после карантина'
                 union all
                 select 'dacha', 'На даче до конца лета'
                 union all
                 select '0', 'Абонент другого региона'
                 union all
                 select 'otkaz_26', 'Устраивает текущий тариф/провайдер'
                 union all
                 select 'otkaz_12', 'Не устраивает качество услуг'
                 union all
                 select 'otkaz_3', 'Не устраивает способ подключения'
                 union all
                 select 'otkaz_1', 'Дорого'
                 union all
                 select 'otkaz_16', 'Пенсионер / Ребенок'
                 union all
                 select 'otkaz_44', 'Не целевой абонент'
                 union all
                 select 'otkaz_28', 'Много раз звонили'
                 union all
                 select 'otkaz_31', 'Ошибка'
                 union all
                 select 'otkaz_23', 'Автоответчик'
                 union all
                 select 'otkaz_42', 'Обрыв разговора'
                 union all
                 select 'otkaz_48', 'Ошибся номером'
                 union all
                 select 'otkaz_45', 'Отказ от заявки'
                 union all
                 select 'otkaz_52', 'Перевел на ответственного'
                 union all
                 select 'otkaz_55', 'Перевел на горячую линию'
                 union all
                 select 'otkaz_43', 'Уточнял условия акции'
                 union all
                 select 'otkaz_56', 'Уточнял причину звонка'
                 union all
                 select 'disp_1', 'В графике (диспетчер)'
                 union all
                 select 'disp_2', 'Проверка статуса (диспетчер)'
                 union all
                 select 'disp_3', 'Отказ от заявки (диспетчер)'
                 union all
                 select 'otkaz_57', 'Подумает, перезвонит сам'
                 union all
                 select 'otkaz_58', 'ГП на адресе'
                 union all
                 select 'otkaz_lb1', 'Отказ'
                 union all
                 select 'otkaz_lb2', 'Отказ передумал'
                 union all
                 select 'otkaz_lb3', 'Не ближайшее время'
                 union all
                 select 'otkaz_cs', 'Курьерская служба'
                 union all
                 select 'no_ansver', 'Нет ответа'),
     result_call as (select 'null_status' result_call, ' ' Результат_звонка
                     union all
                     select 'refusing', 'Отказ'
                     union all
                     select 'MeetingWait', 'Назначена заявка'
                     union all
                     select 'CallWait', 'Назначен звонок'
                     union all
                     select 'NoAnswer', 'Нет ответа'
                     union all
                     select 'Wait', 'Не дождался ответа'
                     union all
                     select 'no_active', 'Неактивный номер'
                     union all
                     select 'stoplist', 'Стоп-лист'
                     union all
                     select '0', 'НЕ ЛПР (RO)'
                     union all
                     select '1', 'Не стоит разговаривать (RO)'
                     union all
                     select '2', 'Нет тех. возможности (RO)'),
     ZH_calls as (select date_start 'Дата окончания',
                         asterisk_caller_id_c,
                         duration_minutes,
                         Результат_звонка,
                         Причина_отказа
                  from (select date_start,
                               duration_minutes,
                               result_call_c,
                               otkaz_c,
                               asterisk_caller_id_c,
                               row_number() over (partition by asterisk_caller_id_c order by date_start desc) rw
                        from (
                                 select date_start,
                                        duration_minutes,
                                        result_call_c,
                                        otkaz_c,
                                        case
                                            when asterisk_caller_id_c like '+7%'
                                                then concat(8, substring(asterisk_caller_id_c, 3, 10))
                                            when asterisk_caller_id_c like '7%'
                                                then concat(8, substring(asterisk_caller_id_c, 2, 10))
                                            else asterisk_caller_id_c end asterisk_caller_id_c
                                 from suitecrm.calls
                                          left join suitecrm.calls_cstm ON calls_cstm.id_c = calls.id
                                 where name in ('Входящий звонок', 'Исходящий звонок')
                                   and date(date_start) = date(now()) - interval 1 day
                                   and asterisk_caller_id_c is not null
                                   and not (duration_minutes = 0
                                     and otkaz_c is null
                                     and result_call_c = 'null_status')) T) T2
                           left join otkaz_c ON otkaz_c.otkaz_c = T2.otkaz_c
                           left join result_call ON result_call.result_call = T2.result_call_c
                  where rw = 1),
     type as (select 'interested' type, 'Интересно' Тип
              union all
              select '1', 'Интересно+'
              union all
              select 'recall', 'Перезвон'
              union all
              select 'recall_plus', 'Перезвон +'
              union all
              select 'meeting', 'Заявка'
              union all
              select 'question', 'Вопрос'
              union all
              select 'perenos', 'Перенос доставки'
              union all
              select 'meeting_konvergent', 'Заявка-Конвергент'
              union all
              select 'question_konvegrent', 'Вопрос-Конвергент'
              union all
              select 'waiters', 'Ждун'
              union all
              select 'waiter2', 'Ждун с обрыва'
              union all
              select 'leads', 'Лиды'
              union all
              select 'recall_far', 'Перезвон Дальний'
              union all
              select 'recall_tomorrow', 'Перезвон Завтра'
              union all
              select 'recall_today', 'Перезвон Сегодня'
              union all
              select 'recall_Inaccurate', 'Перезвон Неточный'
              union all
              select 'r1', 'Перезвон - ПН'
              union all
              select 'r2', 'Перезвон - ВТ'
              union all
              select 'r3', 'Перезвон - СР'
              union all
              select 'r4', 'Перезвон - ЧТ'
              union all
              select 'r5', 'Перезвон - ПТ'
              union all
              select 'r6', 'Перезвон - СБ'
              union all
              select 'r7', 'Перезвон - ВС'),
     config as (select D.name                                        'Название диалога',
                       step.description,
                       step.name
#                            'Название шага перевода'
                        ,
                       index_number                                  'step',
                       replace(replace(queue, '_NEW^', ''), '^', '') 'dialogs',
                       custom_queue_c                                'Принимающая очередь'
                from suitecrm.jc_robconfig_step step
                         left join suitecrm.jc_robconfig_step_cstm cstm on step.id = cstm.id_c
                         left join suitecrm.jc_robconfig_dialog_jc_robconfig_step_c sv
                                   on step.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_step_idb
                         left join suitecrm.jc_robconfig_dialog D
                                   on D.id = sv.jc_robconfig_dialog_jc_robconfig_stepjc_robconfig_dialog_ida
#                 where action_type > 0
     ),
     tabeue as (select step,
                       dialogs,
                       name,
                       description,
                       SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 1), ',', -1) as queue_1,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 1), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1)
                           end                                                    as queue_2,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 2), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1)
                           end                                                    as queue_3,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 3), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1)
                           end                                                    as queue_4,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 4), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1)
                           end                                                    as queue_5,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 5), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1)
                           end                                                    as queue_6,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 6), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1)
                           end                                                    as queue_7,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 7), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1)
                           end                                                    as queue_8,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 8), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1)
                           end                                                    as queue_9,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 9), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1)
                           end                                                    as queue_10,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 10), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1)
                           end                                                    as queue_11,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 11), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1)
                           end                                                    as queue_12,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 12), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1)
                           end                                                    as queue_13,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 13), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1)
                           end                                                    as queue_14,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 14), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1)
                           end                                                    as queue_15,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 15), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1)
                           end                                                    as queue_16,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 16), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1)
                           end                                                    as queue_17,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 17), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1)
                           end                                                    as queue_18,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 18), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1)
                           end                                                    as queue_19,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 19), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                           end                                                    as queue_20,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1)
                           end                                                    as queue_21,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 21), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1)
                           end                                                    as queue_22,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 22), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1)
                           end                                                    as queue_23,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 23), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1)
                           end                                                    as queue_24,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 24), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1)
                           end                                                    as queue_25,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 25), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1)
                           end                                                    as queue_26,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 26), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1)
                           end                                                    as queue_27,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 27), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1)
                           end                                                    as queue_28,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 28), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1)
                           end                                                    as queue_29,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 30), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 29), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 30), ',', -1)
                           end                                                    as queue_30,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 20), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1)
                           end                                                    as queue_31,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 31), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1)
                           end                                                    as queue_32,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 32), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1)
                           end                                                    as queue_33,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 33), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1)
                           end                                                    as queue_34,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 34), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1)
                           end                                                    as queue_35,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 35), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1)
                           end                                                    as queue_36,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 36), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1)
                           end                                                    as queue_37,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 37), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1)
                           end                                                    as queue_38,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 38), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1)
                           end                                                    as queue_39,
                       case
                           when
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 40), ',', -1) =
                                   SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 39), ',', -1)
                               then null
                           else SUBSTRING_INDEX(SUBSTRING_INDEX(dialogs, ',', 40), ',', -1)
                           end                                                    as queue_40
                from config
                where dialogs is not null
                  and dialogs != ''),
     steps as (select *
               from (
                        select step,
                               queue_1 as ochered,
                               name,
                               description
                        from tabeue
                        union all
                        select step, queue_2, name, description
                        from tabeue
                        union all
                        select step, queue_3, name, description
                        from tabeue
                        union all
                        select step, queue_4, name, description
                        from tabeue
                        union all
                        select step, queue_5, name, description
                        from tabeue
                        union all
                        select step, queue_6, name, description
                        from tabeue
                        union all
                        select step, queue_7, name, description
                        from tabeue
                        union all
                        select step, queue_8, name, description
                        from tabeue
                        union all
                        select step, queue_9, name, description
                        from tabeue
                        union all
                        select step, queue_10, name, description
                        from tabeue
                        union all
                        select step, queue_11, name, description
                        from tabeue
                        union all
                        select step, queue_12, name, description
                        from tabeue
                        union all
                        select step, queue_13, name, description
                        from tabeue
                        union all
                        select step, queue_14, name, description
                        from tabeue
                        union all
                        select step, queue_15, name, description
                        from tabeue
                        union all
                        select step, queue_16, name, description
                        from tabeue
                        union all
                        select step, queue_17, name, description
                        from tabeue
                        union all
                        select step, queue_18, name, description
                        from tabeue
                        union all
                        select step, queue_19, name, description
                        from tabeue
                        union all
                        select step, queue_20, name, description
                        from tabeue
                        union all
                        select step, queue_21, name, description
                        from tabeue
                        union all
                        select step, queue_22, name, description
                        from tabeue
                        union all
                        select step, queue_23, name, description
                        from tabeue
                        union all
                        select step, queue_24, name, description
                        from tabeue
                        union all
                        select step, queue_25, name, description
                        from tabeue
                        union all
                        select step, queue_26, name, description
                        from tabeue
                        union all
                        select step, queue_27, name, description
                        from tabeue
                        union all
                        select step, queue_28, name, description
                        from tabeue
                        union all
                        select step, queue_29, name, description
                        from tabeue
                        union all
                        select step, queue_30, name, description
                        from tabeue
                        union all
                        select step, queue_31, name, description
                        from tabeue
                        union all
                        select step, queue_32, name, description
                        from tabeue
                        union all
                        select step, queue_33, name, description
                        from tabeue
                        union all
                        select step, queue_34, name, description
                        from tabeue
                        union all
                        select step, queue_35, name, description
                        from tabeue
                        union all
                        select step, queue_36, name, description
                        from tabeue
                        union all
                        select step, queue_37, name, description
                        from tabeue
                        union all
                        select step, queue_38, name, description
                        from tabeue
                        union all
                        select step, queue_39, name, description
                        from tabeue
                        union all
                        select step, queue_40, name, description
                        from tabeue
                    ) as t2
               where ochered is not null),

     osnova as (select substring(jrl.dialog, 11, 4)     Диалог_лиды,
                       if(substring(log.dialog, 11, 4) is null, substring(jrl.dialog, 11, 4),
                          substring(log.dialog, 11, 4)) Диалог_лог,
                       if(destination_queue is null,
                          if(substring(log.dialog, 11, 4) is null, substring(jrl.dialog, 11, 4),
                             substring(log.dialog, 11, 4)),
                          destination_queue)            'Принимающая_очередь',
                       last_step                        'Последний_шаг',
                       steps.name,
                       steps.description,
                       Тип,
                       Город,
                       jrl.uniqueid,
                       jrl.phone                        'Номер_телефона',
                       jrl.date_entered                 'Дата_создания',
                       date_start_bp                    'БП',
                       phone_work                       'Заявки',
                       duration_minutes                 'Дл._разговора',
                       Результат_звонка                 'Результат',
                       Причина_отказа                   'Причина_отказа',
                       was_repeat                       'Была_ПТВ',
                       case
                           when ptv_c like '%^3^%'
                               or
                                ptv_c like '%^5^%'
                               or
                                ptv_c like '%^6^%'
                               or
                                ptv_c like '%^10^%'
                               or
                                ptv_c like '%^11^%'
                               or
                                ptv_c like '%^19^%' then 'Наша'
                           else '' end                  'Разметка'
                from suitecrm.jc_robot_leads jrl
                         left join suitecrm.jc_robot_leads_cstm jrlc ON jrl.id = jrlc.id_c
                         left join (select *
                                    from suitecrm_robot.jc_robot_log
                                    where date(call_date) = date(now()) - interval 1 day) log
                                   on (date(jrl.date_entered) = date(log.call_date) and jrl.phone = log.phone)
                         left join (select distinct * from suitecrm.transferred_to_other_queue) tr
                                   on (log.uniqueid = tr.uniqueid and log.phone = tr.phone)
                         left join BP ON BP.phone_bp = jrl.phone
                         left join Meets_90 ON Meets_90.phone_work = jrl.phone
                         left join ZH_calls ON ZH_calls.asterisk_caller_id_c = jrl.phone
                         left join town_c ON town_c.town_c = jrlc.town_c
                         left join type ON type.type = jrl.type
                         left join steps
                                   on (steps.ochered = substring(log.dialog, 11, 4) and log.last_step = steps.step)
                where jrl.type in ('interested','recall','recall_plus', 'recall_tomorrow', 'waiters', 'recall_today', 'leads')
                  and date(jrl.date_entered) = date(now()) - interval 1 day
                  and date_start_bp is null
                  and phone_work is null
                  and (Причина_отказа in ('Автоответчик', 'Нет ответа', 'Обрыв разговора', '') or
                       Причина_отказа is null)
                  and (duration_minutes <= 20 or duration_minutes is null)),
     lids as (select osnova.*
              from osnova)

select *
from lids



