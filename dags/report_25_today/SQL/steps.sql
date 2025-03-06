SELECT REGEXP_SUBSTR(turn, '[0-9]+')                                      as queue,
       steps_inconvenient,
       steps_error,
       steps_refusing,
       recall top_recall,
       reset_greet hello_end,
       x_ptv ntv,
       have_ptv,
       is_subs abonent,
       reset_pres welcome_end
FROM suitecrm.jc_robot_reportconfig
         INNER JOIN suitecrm.jc_robot_reportconfig_cstm ON id = id_C
WHERE deleted = 0