select id_custom,
       ptv,
       ifNull(trim(BOTH ',' FROM replace(replace(concat(toString(ttk_pr), ',', toString(mts_pr), ',', toString(rtk_pr), ',',
                                                 toString(nbn_pr), ',', toString(dom_pr), ',', toString(bln_pr)), '0,',
                                          ''), '0', '')),'') list,
       ifNull(trim(BOTH ',' FROM replace(replace(concat(toString(ttk_p), ',', toString(mts_p), ',', toString(rtk_p), ',',
                                                 toString(nbn_p), ',', toString(dom_p), ',', toString(bln_p)), '0,',
                                          ''), '0', '')),'') list2
        from (select id as id_custom,
             ptv,
             case when ptv_ttk = 1 then ttk when ptv = 2 and ptv_fias_ttk = 1 and alive = 1 then ttk when ptv = 3 and holod_ttk = 1 and alive = 1 then ttk else 0 end ttk_pr,
             case when ptv_mts = 1 then mts when ptv = 2 and ptv_fias_mts = 1 and alive = 1 then mts when ptv = 3 and holod_mts = 1 and alive = 1 then mts else 0 end mts_pr,
             case when ptv_rtk = 1 then rtk when ptv = 2 and ptv_fias_rtk = 1 and alive = 1 then rtk when ptv = 3 and holod_rtk = 1 and alive = 1 then rtk else 0 end rtk_pr,
             case when ptv_nbn = 1 then nbn when ptv = 2 and ptv_fias_nbn = 1 and alive = 1 then nbn when ptv = 3 and holod_nbn = 1 and alive = 1 then nbn else 0 end nbn_pr,
             case when ptv_dom = 1 then dom when ptv = 2 and ptv_fias_dom = 1 and alive = 1 then dom when ptv = 3 and holod_dom = 1 and alive = 1 then dom else 0 end dom_pr,
             case when ptv_bln = 1 then bln when ptv = 2 and ptv_fias_bln = 1 and alive = 1 then bln when ptv = 3 and holod_bln = 1 and alive = 1 then bln else 0 end bln_pr,

             case
                 when ptv_ttk = 1 and ttk is not null then 'ttk'
                 when ptv = 2 and ptv_fias_ttk = 1 and ttk is not null and alive = 1 then 'ttk'
                 when ptv = 3 and holod_ttk = 1 and ttk is not null and alive = 1 then 'ttk'
                 else '0' end                                                                     ttk_p,
             case
                 when ptv_mts = 1 and mts is not null then 'mts'
                 when ptv = 2 and ptv_fias_mts = 1 and mts is not null and alive = 1 then 'mts'
                 when ptv = 3 and holod_mts = 1 and mts is not null and alive = 1 then 'mts'
                 else '0' end                                                                     mts_p,
             case
                 when ptv_rtk = 1 and rtk is not null then 'rtk'
                 when ptv = 2 and ptv_fias_rtk = 1 and rtk is not null and alive = 1 then 'rtk'
                 when ptv = 3 and holod_rtk = 1 and rtk is not null and alive = 1 then 'rtk'
                 else '0' end                                                                     rtk_p,
             case
                 when ptv_nbn = 1 and nbn is not null then 'nbn'
                 when ptv = 2 and ptv_fias_nbn = 1 and nbn is not null and alive = 1 then 'nbn'
                 when ptv = 3 and holod_nbn = 1 and nbn is not null and alive = 1 then 'nbn'
                 else '0' end                                                                     nbn_p,
             case
                 when ptv_dom = 1 and dom is not null then 'dom'
                 when ptv = 2 and ptv_fias_dom = 1 and dom is not null and alive = 1 then 'dom'
                 when ptv = 3 and holod_dom = 1 and dom is not null and alive = 1 then 'dom'
                 else '0' end                                                                     dom_p,
             case
                 when ptv_bln = 1 and bln is not null then 'bln'
                 when ptv = 2 and ptv_fias_bln = 1 and bln is not null and alive = 1 then 'bln'
                 when ptv = 3 and holod_bln = 1 and bln is not null and alive = 1 then 'bln'
                 else '0' end                                                                     bln_p
      from suitecrm_robot_ch.contacts
               left join suitecrm_robot_ch.priority_providers on priority_providers.city_c = contacts.city_c
          and provider = network_provider
          and contacts.ptv = priority_providers.ptv_c
          and contacts.region_c = priority_providers.region_c
      where ptv = {}
      
    ) tt