def create_medium_step_file(name, files_from_sql, uniqueid_medium_folder):
    import pandas as pd
    from steps_report.is_number_route import is_number_route
    from steps_report.medium_step import medium_step

    main_calls_raw = rf'{files_from_sql}{name}'
    medium_step_file = rf'{uniqueid_medium_folder}{name}'

    df = pd.read_csv(main_calls_raw, sep=';', encoding='utf-8')
    df.fillna('unknown', inplace=True)

    df_uniqueid = pd.DataFrame()
    df_uniqueid['uniqueid'] = df['uniqueid']
    df_uniqueid['route'] = df['route'].apply(is_number_route)
    df_uniqueid['medium_step'] = df_uniqueid['route'].apply(medium_step)
    df_uniqueid = df_uniqueid.explode('medium_step')

    df_uniqueid.to_csv(medium_step_file, sep=';', index=False, encoding='utf-8')
