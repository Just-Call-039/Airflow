def data(row):
    if row['База'] == 'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ пїЅпїЅ пїЅпїЅпїЅпїЅ':
        return 'Разметка не Наша'
    elif row['База'] == 'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ пїЅпїЅпїЅпїЅ':
        return 'Разметка Наша'
    elif row['База'] == 'пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ пїЅпїЅпїЅпїЅпїЅ':
        return 'Запланированный холод'
    elif row['База'] == 'пїЅпїЅпїЅ пїЅпїЅ пїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅпїЅ':
        return 'Был на операторе'
    elif row['База'] == 'пїЅпїЅпїЅпїЅпїЅ':
        return 'Холод'
    elif row['База'] == 'Р Р°Р·РјРµС‚РєР° РќР°С€Р°':
        return 'Разметка Наша'
    elif row['База'] == 'РҐРѕР»РѕРґ':
        return 'Холод'
    elif row['База'] == 'Р Р°Р·РјРµС‚РєР° РЅРµ РќР°С€Р°':
        return 'Разметка не Наша'
    elif row['База'] == 'Р—Р°РїР»Р°РЅРёСЂРѕРІР°РЅРЅС‹Р№ С…РѕР»РѕРґ':
        return 'Запланированный холод'
    elif row['База'] == 'Р‘С‹Р» РЅР° РѕРїРµСЂР°С‚РѕСЂРµ':
        return 'Был на операторе'
    else :
        return row['База']
    
def network(row):
    if row['network_provider'] == 'пїЅпїЅпїЅпїЅ2':
        return 'Теле2'
    elif row['network_provider'] == 'пїЅпїЅпїЅ':
        return 'МТС'
    elif row['network_provider'] == 'пїЅпїЅпїЅпїЅпїЅпїЅ':
        return 'Билайн'
    elif row['network_provider'] == 'пїЅпїЅпїЅпїЅпїЅпїЅпїЅ':
        return 'Мегафон'
    elif row['network_provider'] == 'MVNO':
        return 'MVNO'
    elif row['network_provider'] == 'РўРµР»Рµ2':
        return 'Теле2'
    elif row['network_provider'] == 'РњРўРЎ':
        return 'МТС'
    elif row['network_provider'] == 'Р‘РёР»Р°Р№РЅ':
        return 'Билайн'
    elif row['network_provider'] == 'РњРµРіР°С„РѕРЅ':
        return 'Мегафон'
    else :
        return row['network_provider']