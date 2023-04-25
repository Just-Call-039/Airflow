SELECT substring(turn, 11, 4)                                      as queue,
       SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 1), ',', -1) as have_ptv_1,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 1), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1)
           end                                                     as have_ptv_2,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 2), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1)
           end                                                     as have_ptv_3,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 3), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1)
           end                                                     as have_ptv_4,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 4), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1)
           end                                                     as have_ptv_5,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 5), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1)
           end                                                     as have_ptv_6,

       case
           when SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 6), ',', -1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 7), ',', -1)
               then null
           else SUBSTRING_INDEX(SUBSTRING_INDEX(have_ptv, ',', 7), ',', -1)
           end                                                     as have_ptv_7
FROM suitecrm.jc_robot_reportconfig
         INNER JOIN suitecrm.jc_robot_reportconfig_cstm ON id = id_C
WHERE deleted = 0
  and have_ptv is not null