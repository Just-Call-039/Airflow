def main_transformation(name, files_from_sql, main_folder):
    import pandas as pd
    from steps_report.reset_step import reset_step

    main_calls_raw = rf'{files_from_sql}\{name}'
    main_calls_true = rf'{main_folder}\{name}'

    df = pd.read_csv(main_calls_raw, sep=';', encoding='utf-8')
    df.fillna('unknown', inplace=True)
    df['reset_step'] = df['route'].apply(reset_step)

    df.to_csv(main_calls_true, sep=';', index=False, encoding='utf-8')
