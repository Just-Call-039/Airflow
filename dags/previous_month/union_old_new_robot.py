import pandas as pd

def union_calls(old_calls_path, new_calls_path, type_dict, union_calls_path):

    old_calls = pd.read_csv(old_calls_path, dtype = type_dict)
    new_calls = pd.read_csv(new_calls_path, dtype = type_dict)
    print('old_calls size: ', old_calls.shape[0])
    print('new_calls size: ', new_calls.shape[0])

    union_calls = pd.concat([old_calls, new_calls], ignore_index = True)
    print('union_calls size: ', union_calls.shape[0])
    union_calls.to_csv(union_calls_path, index = False)
