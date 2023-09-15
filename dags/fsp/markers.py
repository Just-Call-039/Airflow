def markers_archive():
    import pandas as pd
    import glob
    import os

    path = r'\\10.88.22.128\dbs\scripts fsp\Current Files\ФСП\Трафик'
    files = sorted(glob.glob(path + "/*.csv"), reverse=True)
    data = pd.read_csv(r'\\10.88.22.128\dbs\scripts fsp\Current Files\Маркера.csv')
    n = 0
    num_of_files = len(os.listdir(path))

    print(f'Всего файлов {num_of_files}')

    for i in files:
        n += 1
        print(i)
        chunk = pd.read_csv(i)
        chunk = chunk[['marker']].drop_duplicates()
        data = pd.concat([data,chunk])
        data = data.drop_duplicates()
        
        if n == 5:
            break

    data.to_csv(r'\\10.88.22.128\dbs\scripts fsp\Current Files\Маркера.csv', index=False)