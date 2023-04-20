import pandas as pd

ptv_nasha = ['^5^', '^6^', '^3^', '^10^', '^11^', '^19^', ]
ptv_ne_nasha = ['^5_15^', '^5_16^', '^5_17^', '5_18^', '^5_19^', '^5_20^', '^5_21^',
                '^6_15^', '^6_16^', '^6_17^', '6_18^', '^6_19^', '^6_20^', '^6_21^',
                '^3_15^', '^3_16^', '^3_17^', '3_18^', '^3_19^', '^3_20^', '^3_21^',
                '^10_15^', '^10_16^', '^10_17^', '10_18^', '^10_19^', '^10_20^', '^10_21^',
                '^11_15^', '^11_16^', '^11_17^', '11_18^', '^11_19^', '^11_20^', '^11_21^',
                '^19_15^', '^19_16^', '^19_17^', '19_18^', '^19_19^', '^19_20^', '^19_21^']

def region(row):
    if any(w in row['ptv_c'] for w in ptv_nasha):
        return 'ptv_1'
    elif any(w in row['ptv_c'] for w in ptv_ne_nasha):
        return 'ptv_2'
    else:
        return row['region_c']
def date(row):
    if row['datecall'] == 0:
        return row['date_entered']
    return row['datecall']
def queue(row):
    if row['queue'] == 0:
        return row['last_queue_c']
    else:
        return row['queue']
def destination_queue(row):
    if row['destination_queue'] == 0:
        return row['last_queue_c']
    else:
        return row['destination_queue']
def meet_proect(row):
    if row['proect'] in {'RTK', 'RTK LIDS'}:
        return 'RTK'
    elif row['proect'] in {'TTK', 'TTK LIDS'}:
        return 'TTK'
    elif row['proect'] in {'DOMRU', 'DOMRU LIDS', 'DOMRU Dop'}:
        return 'DOMRU'
    elif row['proect'] in {'MTS', 'MTS LIDS'}:
        return 'MTS'
    elif row['proect'] in {'NBN', 'NBN LIDS'}:
        return 'NBN'
    elif row['proect'] in {'BEELINE', 'BEELINE LIDS'}:
        return 'BEELINE'
    else:
        return 'DR'
def check_team_project(row):
    if row['team_project'] in {'RTK', 'RTK LIDS'}:
        return 'RTK'
    elif row['team_project'] in {'TTK', 'TTK LIDS'}:
        return 'TTK'
    elif row['team_project'] in {'DOMRU', 'DOMRU LIDS', 'DOMRU Dop'}:
        return 'DOMRU'
    elif row['team_project'] in {'MTS', 'MTS LIDS'}:
        return 'MTS'
    elif row['team_project'] in {'NBN', 'NBN LIDS'}:
        return 'NBN'
    elif row['team_project'] in {'BEELINE', 'BEELINE LIDS'}:
        return 'BEELINE'
    else:
        return 'DR'
def meet_proect_final(row):
    if row['team_project'] == '0':
        return row['proect']
    elif row['check_team_project'] == row['module']:
        return row['team_project']
    else:
        return row['proect']