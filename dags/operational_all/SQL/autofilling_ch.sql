CREATE TABLE IF NOT EXISTS autofilling ON CLUSTER '{cluster}'
                    (
                    datetime          DateTime, 
                    mother_queue      String, 
                    ro_queue          String, 
                    campaign          String, 
                    base_hashtag      String,
                    name_autofilling  String, 
                    marker            String, 
                    status            String, 
                    base_count        String, 
                    limits            Int64,
                    facts             Int64
                    
                    )
                    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/suitecrm_robot_ch/autofilling', '{replica}')
                    ORDER BY (datetime, mother_queue, campaign, marker)
                    SETTINGS index_granularity = 8192;

