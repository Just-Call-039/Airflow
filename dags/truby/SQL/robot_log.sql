
select calldate,
        queue,
        group_trafic2,
        last_step,
        trunk_id,
        network_provider,
        city_c,
        otkaz_23,
        phone,
        trafic,
        server_number,
        billsec,
        real_billsec,
        hour,
        minute
from (select calldate,
                queue,
                phone,
                uniqueid,
                last_step,
                billsec,
                real_billsec,
                trunk_id,
                network_provider,
                server_number,
                city_c,
                otkaz_23,
                trafic,
                (hour+3) hour,
                minute,
                if(trafic >= 100, 100, trafic) group_trafic2
        from (select date(call_date)                                 calldate,
        hour(call_date)                                 hour,
        minute(call_date)                                 minute,
                    phone,
                    uniqueid,
                    last_step,
                    billsec,
                    real_billsec,
                    trunk_id,
                    server_number,
                    network_provider_c,
                    substring(dialog,11,4) queue,
                    if(otkaz = 'otkaz_23',1,0) otkaz_23,
                    city_c,
                    if(real_billsec is NULL, billsec, real_billsec) trafic,
                    case
                        when network_provider_c = '83' then 'МТС'
                        when network_provider_c = '80' then 'Билайн'
                        when network_provider_c = '82' then 'Мегафон'
                        when network_provider_c = '10' then 'Теле2'
                        when network_provider_c = '68' then 'Теле2'
                        else 'MVNO'
                        end                                         network_provider
            from suitecrm_robot.jc_robot_log
            where date(call_date) = date(now()) - interval {} day
                and (inbound_call = 0 or inbound_call = ''
                union all

                select date(call_date)                                 calldate,
                    hour(call_date)                                 hour,
                    minute(call_date)                                 minute,
                    phone,
                    dialog_id uniqueid,
                    last_step,
                    billsec,
                    real_billsec,
                    trunk_id,
                    server_number,
                    network_provider network_provider_c,
                    robot_id queue,
                    if(refuse = 'otkaz_23',1,0) otkaz_23,
                    city city_c, 
                    if(real_billsec is NULL, billsec, real_billsec) trafic,
                    case
                        when network_provider = '83' then 'МТС'
                        when network_provider = '80' then 'Билайн'
                        when network_provider = '82' then 'Мегафон'
                        when network_provider = '10' then 'Теле2'
                        when network_provider = '68' then 'Теле2'
                        else 'MVNO'
                        end                                         network_provider
            from suitecrm_robot.robot_log 
                    left join suitecrm_robot.robot_log_addition 
                    on robot_log.id = robot_log_addition.robot_log_id
            where date(call_date) = date(now()) - interval {} day
                and (direction = 0 or direction = '')
    
            ) r_log) r_log2