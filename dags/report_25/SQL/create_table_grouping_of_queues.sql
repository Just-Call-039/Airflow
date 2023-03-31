CREATE TABLE suitecrm_robot_ch.grouping_of_queues
(
    date_add date,
    queue    Int,
    group    String,
    project  String
) ENGINE = MergeTree()
      order by date_add;

-- DROP TABLE suitecrm_robot_ch.request_25;
