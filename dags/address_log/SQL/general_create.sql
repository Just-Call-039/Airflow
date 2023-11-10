create table suitecrm_robot_ch.address_log
(
    uniqueid  String,
    calldate  DateTime,
    phone     String,
    dialog    String,
    city      String,
    street    String,
    house     String,
    korp      String,
    providers String
) ENGINE = MergeTree
      order by uniqueid