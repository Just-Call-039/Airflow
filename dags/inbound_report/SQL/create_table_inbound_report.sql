CREATE TABLE inbound_report ON CLUSTER '{cluster}'   
                                            
                        

                                        (
                                        `call_date`            DateTime, 
                                        `phone`                String, 
                                        `billsec_t`            Int64, 
                                        `lastapp_t`            String, 
                                        `active`               String,
                                        `exit_point`           String,
                                        `project`              String,
                                        `count`                Int64,
                                        `daily_count`          Int64,
                                        `spam`                 Int8, 
                                        `date_r`               String,
                                        `last_step`            String, 
                                        `queue_i`              String, 
                                        `billsec_r`            Int64, 
                                        `queue_r`              String,
                                        `date_c`               String,
                                        `otkaz_c`              String, 
                                        `queue_c`              String, 
                                        `request_r`            Int8, 
                                        `type_step`            Int8,
                                        `request_c`            Int8

                                        )

     
                                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/suitecrm_robot_ch/inbound_report', '{replica}')
                                ORDER BY call_date