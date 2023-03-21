# Функция для отбора только промежуточных шагов.
# Необходимо передать строку с запятыми (полный роут).


def medium_step(my_str):
    if len(my_str) in (0, 1):
        return 'empty'
    else:
        return my_str[:-1]
