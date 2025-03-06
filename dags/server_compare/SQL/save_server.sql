CREATE TABLE IF NOT EXISTS server_compare ON CLUSTER '{cluster}'

            (
            date                    Date,
            last_step               String, 
            dialog                  String,
            server_number           String,
            autootvetchik           Int8,
            client_status           String,
            directory               String,
            phone                   String,
            found                   Float64,
            search_sec              Float64,
            sqltook_sec             Float64,
            marker                  String,
            real_billsec            Int64,
            trunk_id                String,
            network_provider_c      String,
            city                    String,
            town                    String,
            perevod                 Int8,
            perevod_done            Int8,
            request                 Int8,
            project                 String,
            quality                 String
            )

            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/suitecrm_robot_ch/server_compare', '{replica}')
                                ORDER BY (date, server_number, dialog) 
                                SETTINGS index_granularity = 8192;
