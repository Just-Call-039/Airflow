CREATE TABLE suitecrm_robot_ch.request_25
(
    phone_number String,
    assigned_user_id String,
    status_request String,
    date_request DateTime,
    uniqueid String,
    ochered Int32,
    project String
) ENGINE = MergeTree()
    order by date_request;

DROP TABLE suitecrm_robot_ch.request_25;
