CREATE TABLE IF NOT EXISTS contact_datebase ON CLUSTER '{cluster}'
                    (
                    region_c                String, 
                    network_provider_c      String, 
                    base_source_c           String, 
                    town_c                  String, 
                    city_c                  String,
                    ntv_ptv                 Int8, 
                    step_c                  String, 
                    priority1               String, 
                    priority2               String, 
                    last_call               Date, 
                    bln_nasha               Int8,
                    mts_nasha               Int8, 
                    nbn_nasha               Int8, 
                    dom_nasha               Int8, 
                    rtk_nasha               Int8, 
                    ttk_nasha               Int8,
                    phone                   String, 
                    stoplist_c              String, 
                    ptv_n                   Int8, 
                    contacts                Int64, 
                    rest_days               Int64 
                    )

                    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/suitecrm_robot_ch/contact_datebase', '{replica}')
                    ORDER BY (last_call, phone)
                    SETTINGS index_granularity = 8192;

