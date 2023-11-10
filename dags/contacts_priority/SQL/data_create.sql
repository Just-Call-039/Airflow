create table suitecrm_robot_ch.data
(
    town_c                 Nullable(String),
    city_c                 Nullable(String),
    network_provider       String,
    region                 Nullable(String),
    general_stop           Nullable(String),
    ntv_ptv                Nullable(String),
    ntv_step               Nullable(String),
    ptvdom                 Nullable(String),
    ptvrtk                 Nullable(String),
    ptvttk                 Nullable(String),
    ptvbln                 Nullable(String),
    ptvmts                 Nullable(String),
    ptvnbn                 Nullable(String),
    ptv2com                Nullable(String),
    ptvfias_dom            Nullable(String),
    ptvfias_rtk            Nullable(String),
    ptvfias_ttk            Nullable(String),
    ptvfias_bln            Nullable(String),
    ptvfias_mts            Nullable(String),
    ptvfias_nbn            Nullable(String),
    ptvfias_2com           Nullable(String),
    stop_connected_bln     Nullable(String),
    stop_connected_mts     Nullable(String),
    stop_connected_2com    Nullable(String),
    stop_connected_nbn     Nullable(String),
    stop_connected_dom     Nullable(String),
    stop_connected_rtk     Nullable(String),
    stop_connected_ttk     Nullable(String),
    stop_ro_connected_bln  Nullable(String),
    stop_ro_connected_mts  Nullable(String),
    stop_ro_connected_2com Nullable(String),
    stop_ro_connected_nbn  Nullable(String),
    stop_ro_connected_dom  Nullable(String),
    stop_ro_connected_rtk  Nullable(String),
    stop_ro_connected_ttk  Nullable(String),
    stop_ntv_bln           Nullable(String),
    stop_ntv_mts           Nullable(String),
    stop_ntv_2com          Nullable(String),
    stop_ntv_nbn           Nullable(String),
    stop_ntv_dom           Nullable(String),
    stop_ntv_rtk           Nullable(String),
    stop_ntv_ttk           Nullable(String),
    stop_ntv_operator_bln  Nullable(String),
    stop_ntv_operator_mts  Nullable(String),
    stop_ntv_operator_2com Nullable(String),
    stop_ntv_operator_nbn  Nullable(String),
    stop_ntv_operator_dom  Nullable(String),
    stop_ntv_operator_rtk  Nullable(String),
    stop_ntv_operator_ttk  Nullable(String),
    stop_stop_bln          Nullable(String),
    stop_stop_mts          Nullable(String),
    stop_stop_2com         Nullable(String),
    stop_stop_nbn          Nullable(String),
    stop_stop_dom          Nullable(String),
    stop_stop_rtk          Nullable(String),
    stop_stop_ttk          Nullable(String),
    stop_status            Nullable(String),
    stop_otkaz             Nullable(String),
    city_name              Nullable(String),
    town_name              Nullable(String),
    region_name            Nullable(String),
    agreed_rtk             Nullable(String),
    ptv_nasha              Nullable(String),
    ptv_ne_nasha           Nullable(String),
    ptv                    Nullable(String),
    stop_dom               Nullable(String),
    stop_rtk               Nullable(String),
    stop_ttk               Nullable(String),
    stop_bln               Nullable(String),
    stop_mts               Nullable(String),
    stop_nbn               Nullable(String),
    stop_2com              Nullable(String),
    alive                  Nullable(String),
    priority1              Nullable(String),
    priority2              Nullable(String),
    rest_days              Nullable(Int64),


    contacts               Nullable(Int64)


) ENGINE = MergeTree
      order by network_provider