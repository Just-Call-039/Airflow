def perevod(row):
    if row['step'] == '':
        return 0
    else:
        return 1
    
def perevelys(row):
    if row['step'] == '':
        return 0
    elif row['assigned_user_id'] in ['1','',' ','0']:
        return 0
    else:
        return 1
    
def etv(row):
    for i in row['have_ptv'].split(','):
        if i in row['route'].split(','):
            return i
        else:
            return '0'
        
def last_step(row):
    for i in row['steps_inconvenient'].split(','):
        if i == row['last_step']:
            return 'steps_inconvenient'
        
    for i in row['steps_error'].split(','):
        if i == row['last_step']:
            return 'steps_error'
        
    for i in row['steps_refusing'].split(','):
        if i == row['last_step']:
            return 'steps_refusing'
        
    for i in row['top_recall'].split(','):
        if i == row['last_step']:
            return 'top_recall'
        
    for i in row['hello_end'].split(','):
        if i == row['last_step']:
            return 'hello_end'
        
    for i in row['welcome_end'].split(','):
        if i == row['last_step']:
            return 'welcome_end'
        
    for i in row['ntv'].split(','):
        if i == row['last_step']:
            return 'ntv'
        
    for i in row['abonent'].split(','):
        if i == row['last_step']:
            return 'abonent'
        
# def steps_inconvenient(row):
#     for i in row['steps_inconvenient'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def steps_error(row):
#     for i in row['steps_error'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def steps_refusing(row):
#     for i in row['steps_refusing'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def top_recall(row):
#     for i in row['top_recall'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def hello_end(row):
#     for i in row['hello_end'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def welcome_end(row):
#     for i in row['welcome_end'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def ntv(row):
#     for i in row['ntv'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
# def abonent(row):
#     for i in row['abonent'].split(','):
#         if i == row['last_step']:
#             return 1
#         else:
#             return 0
        
def network_provider_c(i):
    if i in {'10','68'}:
        return 'Теле 2'
    elif i == '80':
        return 'Билайн'
    elif i == '82':
        return 'Мегафон'
    elif i == '83':
        return 'МТС'
    else:
        return 'MVNO'
    
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