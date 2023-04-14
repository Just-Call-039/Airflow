# Функция для очистки роута от текстовых значений.
# Необходимо передать строку с запятыми (полный роут).


def is_number_route(my_str):
    new_list = []
    temp = my_str.strip().split(',')
    # Перебор всех элементов списка.
    for i in temp:
        # И оставляем только числовые значения.
        if i.isdigit():
            new_list.append(int(i))
    return new_list
