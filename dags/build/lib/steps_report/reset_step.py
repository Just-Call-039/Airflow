# Функция для отбора шага сброса (последний шаг роута).
# Необходимо передать строку с запятыми (полный роут).


def reset_step(my_str):
    my_str = my_str.strip().split(',')
    if len(my_str) == 0:
        return 'empty'
    else:
        return my_str[-1]
