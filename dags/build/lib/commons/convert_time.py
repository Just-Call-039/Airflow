# Функция для преобразования секунд в дни, часы, минуты, секунды.
# Необходимо передать количество секунд.
# Секунды округляет до двух знаков после запятой.


def convert_time(number):
    from decimal import Decimal

    number = float(number)
    my_sec = Decimal(str(number % 60))
    my_sec = my_sec.quantize(Decimal("1.00"))
    if number <= 60:
        return f'{my_sec} сек.'
    else:
        my_min = int(number // 60)
        if my_min >= 60:
            my_hour = int(my_min // 60)
            my_min = my_min % 60
            if my_hour >= 24:
                my_day = int(my_hour // 24)
                my_hour = my_hour % 24
                return f'{my_day} дн., {my_hour} ч., {my_min} мин., {my_sec} сек.'
            return f'{my_hour} ч., {my_min} мин., {my_sec} сек.'
        return f'{my_min} мин., {my_sec} сек.'
