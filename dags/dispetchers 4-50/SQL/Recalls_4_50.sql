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
                select 92, '92 Севастополь'), /*словарь*/
     recall as (select 'reason_compare' as                                                   recall_reason,
                       'Подумаю. Клиент хочет сравнить продукт с аналогичными предложениями' Причина
                union all
                select 'reason_think', 'Подумаю. Клиенту нужно подумать'
                union all
                select 'reason_information', 'Подумаю. Клиенту требуется дополнительная информация'
                union all
                select 'reason_inconvenient', 'Подумаю. Клиенту не удобно говорить (после презентации)'
                union all
                select 'reason_consult', 'Подумаю. Клиент хочет посоветоваться'
                union all
                select 'transfer_before', 'Перенос звонка (до презентации)'
                union all
                select 'transfer_after', 'Перенос звонка (после презентации)'
                union all
                select 'nedozvon', 'Выбыл из обзвона по причине недозвона'
                union all
                select 'off_think', 'Выбыл из обзвона по попыткам подумаю'
                union all
                select 'off_transfer', 'Выбыл из обзвона по попыткам перенос звонка'
                union all
                select 'off_error', 'Выбыл из обзвона по попыткам сбой'
                union all
                select 'm_ready', 'Готов к сделке'
                union all
                select 'm_think_after', 'Презентация была, думает'
                union all
                select 'm_busy_after', 'Презентация была, некогда'
                union all
                select 'm_busy', 'Некогда'
                union all
                select 'mnp_ready', 'БЛНМНП Готов к сделке'
                union all
                select 'mnp_think_after', 'БЛНМНП Презентация была, думает'
                union all
                select 'mnp_busy_after', 'БЛНМНП Презентация была, некогда'
                union all
                select 'mnp_busy', 'БЛНМНП Некогда'
                union all
                select 'recall_responsible', 'Перезвонить ответственному'),/*словарь*/
     status as (select 0 status, 'Запланирован' Статус
                union all
                select 1, 'Выполнен'
                union all
                select 2, 'Отправлен в автодозвон'
                union all
                select 3, 'Автоматически удалён'),/*словарь*/
     BP as (select phone phone_bp, date_start date_start_bp
            from suitecrm.jc_planned_calls jpc
            where date(date_start) > date(now())- interval {} day),
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
                 select 'no_ansver', 'Нет ответа'),/*словарь*/
     result_call as (select 'refusing' result_call, 'Отказ' Результат_звонка
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
                     select '2', 'Нет тех. возможности (RO)'),/*словарь*/
     ZH_calls as (select date_start 'Дата окончания',
                         asterisk_caller_id_c,
                         duration_minutes,
                         Результат_звонка,
                         result_call_c,
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
                                 where (name in ('Исходящий звонок','') or (name = 'Входящий звонок' and direction = 'I'))
                                   and date(date_start) = date(now())- interval {} day
                                   and asterisk_caller_id_c is not null
                                   and not (duration_minutes = 0
                                     and otkaz_c is null
                                     and result_call_c = 'null_status')) T) T2
                           left join otkaz_c ON otkaz_c.otkaz_c = T2.otkaz_c
                           left join result_call ON result_call.result_call = T2.result_call_c
                  where rw = 1),
     ocheredi_new as (select distinct date,
                                      queue,
                                      case
                                          when project = 11 and queue_project.project_type = 1 then 'MTS LIDS'
                                          when project = 10 and queue_project.project_type = 1 then 'BEELINE LIDS'
                                          when project = 6 and queue_project.project_type = 1 then 'TTK LIDS'
                                          when project = 5 and queue_project.project_type = 1 then 'RTK LIDS'
                                          when project = 3 and queue_project.project_type = 1 then 'DOMRU LIDS'
                                          when project = 19 then 'NBN'
                                          when project = 11 then 'MTS'
                                          when project = 10 then 'BEELINE'
                                          when project = 6 then 'TTK'
                                          when project = 5 then 'RTK'
                                          when project = 3 then 'DOMRU'
                                          else 'DR' end project,
                                      queue_project.mother
                      from suitecrm.queue_project
                      where date >= '2022-01-25'),/*словарь*/
     ocheredi_new2 as (select distinct date,
                                      queue,
                                      case
                                          when project = 11 and queue_project.project_type = 1 then 'MTS LIDS'
                                          when project = 10 and queue_project.project_type = 1 then 'BEELINE LIDS'
                                          when project = 6 and queue_project.project_type = 1 then 'TTK LIDS'
                                          when project = 5 and queue_project.project_type = 1 then 'RTK LIDS'
                                          when project = 3 and queue_project.project_type = 1 then 'DOMRU LIDS'
                                          when project = 19 then 'NBN'
                                          when project = 11 then 'MTS'
                                          when project = 10 then 'BEELINE'
                                          when project = 6 then 'TTK'
                                          when project = 5 then 'RTK'
                                          when project = 3 then 'DOMRU'
                                          else 'DR' end project,
                                      queue_project.mother
                      from suitecrm.queue_project
                      where date >= '2022-01-25'),/*словарь*/
     robot_log as (select phone,
                          if(mother is null, queue, mother) mother,
                          call_date,
                          project,
                          assigned_user_id,
                          last_step
                   from (select jc2.phone,
                                jc2.queue,
                                destination_queue,
                                ocheredi_new.mother,
                                call_date,
                                assigned_user_id,
                                ocheredi_new.project,
                                last_step
                         from (select jc.phone,
                                      assigned_user_id,
                                      substring(jc.dialog, 11, 4) queue,
                                      if(destination_queue is null, substring(jc.dialog, 11, 4),
                                         destination_queue)       destination_queue,
                                      date(call_date)             call_date,
                                      last_step
                               from suitecrm_robot.jc_robot_log jc
                                        left join (select distinct * from suitecrm.transferred_to_other_queue) transferred_to_other_queue
                                                  on jc.uniqueid = transferred_to_other_queue.uniqueid
                               where date(call_date) >= '2022-02-01'
                                 and assigned_user_id not in ('', '1')) jc2
                                  left join ocheredi_new
                                            on (jc2.destination_queue = ocheredi_new.queue and call_date = date)) jc3),
     users_crm as (select *,
                      case
                           when left(first_name, instr(first_name, ' ') - 1) > 0 and
                                left(first_name, instr(first_name, ' ') - 1) < 10000
                               then left(first_name, instr(first_name, ' ') - 1)
                           when left(first_name, 2) = 'я_'
                               then substring(first_name, 3, (instr(first_name, ' ') - 3))
                           when left(first_name, 1) = 'я'
                               then substring(first_name, 2, (instr(first_name, ' ') - 1))
                          when first_name > 0 then first_name
                           else '' end team
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
     osnova as (select distinct
       case when robot_log.project is null and ocheredi_new.project is null then ocheredi_new2.project
            when robot_log.project is null then ocheredi_new.project else robot_log.project end Проект,
       REPLACE(REPLACE(jpc.description, CHAR(13), ','), CHAR(10), ',')   description,
       jpc.assigned_user_id,
       users_crm.fio,
       users_crm.team,
       jpc.date_entered,
       jpc.date_start,
       Статус,
       jpcc.last_queue_c,
       Причина,
       Город,
       jpc.phone,
       date_start_bp,
       phone_work,
       duration_minutes,
       Результат_звонка,
       Причина_отказа,
       last_step Последний_шаг
from suitecrm.jc_planned_calls jpc
         left join suitecrm.jc_planned_calls_cstm jpcc ON jpc.id = jpcc.id_c
         left join town_c ON jpcc.town_c=town_c.town_c
         left join recall ON recall.recall_reason = jpc.recall_reason
         left join status ON status.status = jpc.status
         left join BP ON jpc.phone = BP.phone_bp
         left join Meets_90 ON jpc.phone = Meets_90.phone_work
         left join ZH_calls ON ZH_calls.asterisk_caller_id_c = jpc.phone
         left join (select * from suitecrm.jc_planned_calls jpc
                    left join suitecrm.jc_planned_calls_cstm jpcc ON jpc.id = jpcc.id_c) jp ON jpc.id = jp.id
         left join robot_log ON (jp.phone = robot_log.phone and robot_log.call_date = date(jp.date_entered))
         left join ocheredi_new on (ocheredi_new.date = date(jp.date_entered) and ocheredi_new.mother = jp.last_queue_c)
         left join ocheredi_new2 on (ocheredi_new2.date = date(jp.date_entered) and ocheredi_new2.queue = jp.last_queue_c)
         left join users_crm on jpc.assigned_user_id = users_crm.id
where date(jpc.date_start) = date(now())- interval {} day)

select distinct *
                from
(select Проект,
       description,
       assigned_user_id as 'Ответственный перезвона',
       fio 'ФИО',
       team 'Команда',
       date_entered 'Дата создания',
       date_start    'Дата и время звонка',
       Статус,
       last_queue_c  Очередь,
       Причина,
       Город,
       phone         Телефон,
       date_start_bp  'БП',
       phone_work  'Заявки',
       duration_minutes  'Дл. разговора',
       Результат_звонка  'Результат',
       Причина_отказа  'Причина отказа',
       Последний_шаг 'Последний шаг'
from osnova
where date_start_bp is null
and phone_work is null
and (Причина_отказа in ('Автоответчик','Нет ответа','Обрыв разговора', '') or Причина_отказа is null)
and (duration_minutes <= 20 or duration_minutes is null)
) SS