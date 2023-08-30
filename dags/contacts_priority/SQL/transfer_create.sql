create table suitecrm_robot_ch.contacts_cstm
                    (
                    id                 String,
                    phone_work         String,
                    last_call_c        DateTime,
                    priority1          String,
                    priority2          String,
                    ptv_c              String,
                    next_project       String,
                    last_project       String,
                    stoplist_c         String,
                    base_source_c      String,
                    town_c             String,
                    city_c             String,
                    marker_c           String,
                    step_c             String,
                    last_queue_c       String,
                    region_c           String,
                    network_provider_c String,
                    otkaz_c            String,
                    contacts_status_c  String
                    ) ENGINE = MergeTree
                        order by id