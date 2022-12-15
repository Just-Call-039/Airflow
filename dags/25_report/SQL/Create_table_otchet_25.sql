CREATE TABLE suitecrm_robot_ch.otchet_25
(
    my_date DateTime,
    uniqueid String,
    ochered Int32,
    last_step Int32,
    route String,
    billsec Int32,
    client_status String,
    otkaz String,
    directory String,
    server_number Int32,
    city_c Int32,
    ptv_c String,
    marker Int32,
    was_repeat Int8,
    phone String,
    teh_vozmozhnost String,
    region String,
    status String,
    alive Int8
) ENGINE = MergeTree()
    order by my_date;

DROP TABLE suitecrm_robot_ch.otchet_25;
