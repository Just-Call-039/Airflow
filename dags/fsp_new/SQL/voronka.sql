with other_status as (select parent_id,
                                date_created,
                                before_value_string,
                                after_value_string,
                                jc_meetings_other_audit.created_by
                        from suitecrm.jc_meetings_other_audit
                                left join suitecrm.jc_meetings_other
                                            on jc_meetings_other.id = jc_meetings_other_audit.parent_id
                                left join suitecrm.jc_meetings_other_cstm on id_c = jc_meetings_other_audit.parent_id
                        where jc_meetings_other.project in ('selection', 'hr')
                            and field_name = 'status'
                            and date(date_entered) >= '2023-10-01'),
        other_date_start as (select jc_meetings_other_audit.id,
                                    parent_id,
                                    date_created,
                                    before_value_string,
                                    after_value_string,
                                    jc_meetings_other_audit.created_by
                            from suitecrm.jc_meetings_other_audit
                                    left join suitecrm.jc_meetings_other
                                                on jc_meetings_other.id = jc_meetings_other_audit.parent_id
                                    left join suitecrm.jc_meetings_other_cstm on id_c = jc_meetings_other_audit.parent_id
                            where jc_meetings_other.project in ('selection', 'hr')
                                and field_name = 'date_start'
                                and date(date_entered) >= '2023-10-01'),
        start_join as (select other_status.parent_id,
                            other_status.date_created,
                            other_status.before_value_string     status_before,
                            other_status.after_value_string      status_after,
                            other_date_start.before_value_string date_before,
                            other_date_start.after_value_string  date_after,
                            other_date_start.id                  other_date_start_id
                        from other_status
                                left join other_date_start on other_status.parent_id = other_date_start.parent_id and
                                                            other_status.date_created = other_date_start.date_created and
                                                            other_status.created_by = other_date_start.created_by),
        final_join as (select parent_id,
                            date_created,
                            status_before,
                            status_after,
                            date_before,
                            date_after
                        from start_join
                        union all
                        select parent_id,
                            date_created,
                            ''                                   status_before,
                            ''                                   status_after,
                            other_date_start.before_value_string date_before,
                            other_date_start.after_value_string  date_after
                        from other_date_start
                        where id not in (select other_date_start_id
                                        from start_join) )

    select parent_id,
                            date_created,
                            status_before,
                            status_after,
                            date_before,
                            date_after
    from final_join