create table suitecrm_robot_ch.contacts
                        (   id String,
                            phone_work Nullable(String),
                            mobile_def Nullable(Int8),
                            city_c Nullable(String),
                            town_c Nullable(String),
                            region_c Nullable(String),
                            network_provider Nullable(String),
                            agreed_rtk Nullable(Int8),
                            ptv Nullable(Int8),

                            ptv_dom Nullable(Int8),
                            ptv_rtk Nullable(Int8),
                            ptv_ttk Nullable(Int8),
                            ptv_bln Nullable(Int8),
                            ptv_mts Nullable(Int8),
                            ptv_nbn Nullable(Int8),

                            ptv_fias_dom Nullable(Int8),
                            ptv_fias_rtk Nullable(Int8),
                            ptv_fias_ttk Nullable(Int8),
                            ptv_fias_bln Nullable(Int8),
                            ptv_fias_mts Nullable(Int8),
                            ptv_fias_nbn Nullable(Int8),

                            holod_dom Nullable(Int8),
                            holod_rtk Nullable(Int8),
                            holod_ttk Nullable(Int8),
                            holod_bln Nullable(Int8),
                            holod_mts Nullable(Int8),
                            holod_nbn Nullable(Int8),

                            stop_dom Nullable(Int8),
                            stop_rtk Nullable(Int8),
                            stop_ttk Nullable(Int8),
                            stop_bln Nullable(Int8),
                            stop_mts Nullable(Int8),
                            stop_nbn Nullable(Int8),
                            stop_status Nullable(Int8),
                            stop_otkaz Nullable(Int8),
                            general_stop Nullable(Int8),
                            ntv_ptv Nullable(Int8),
                            ntv_step Nullable(Int8),
                            alive Nullable(Int8)

                        ) ENGINE = MergeTree
                        order by id