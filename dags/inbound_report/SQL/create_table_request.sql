CREATE TABLE IF NOT EXISTS request ON CLUSTER '{cluster}'

                (
                 userid                 String,
                 dateentered            DateTime,
                 phone                  String,
                 statused               String,
                 queue              	String,
                 regions                String
                 )

        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/suitecrm_robot_ch/request', '{replica}')
        ORDER BY (dateentered, phone, userid, queue)