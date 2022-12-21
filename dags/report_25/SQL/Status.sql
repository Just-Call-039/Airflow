select substring(turn, 11, 4) as ochered,
       steps_autoanswer       as avtootvetchik,
       steps_transferred      as perevod,
       steps_refusing         as otkaz,
       reset_greet            as sbros_na_privetsvii,
       x_ptv                  as net_teh_vozmozhnosti,
       have_ptv               as est_teh_vozmozhnost,
       reset_pres             as sbros_na_presentacii,
       is_subs                as yavlyaetsya_abonentom,
       steps_inconvenient     as neudobno_govorit,
       steps_error            as oshobka_razgovora
from suitecrm.jc_robot_reportconfig
where deleted = 0;
