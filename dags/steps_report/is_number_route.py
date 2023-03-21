# Функция для очистки роута от текстовых значений.
# Необходимо передать строку с запятыми (полный роут).


def is_number_route(my_str):
    new_list = []
    temp = my_str.strip().split(',')
    for i in temp:
        if i.isdigit():
            new_list.append(int(i))
    return new_list
