def status_dict(status, to_file_status_dict):
    from commons.my_clear import my_c
    from my_status import my_status

    status_dict = {}

    # Исходный файл с очередями и статусами Status.csv.
    # Открытие исходного файла.
    with open(status, encoding='utf-8') as file:
        # Итерация по строкам.
        for now in file:
            # Разделение строки по ";".
            now = [my_c(i) for i in now.split(';')]
            # Если первое значение - слово, передаем статусы в список.
            if now[0].isalpha():
                status = [my_c(i) for i in now]
                continue
            # Далее записываем словарь с ключом - очередь, значениями - последний шаг,
            # к последнему шагу - код (индекс статуса из списка со статусами).
            status_dict[now[0]] = {now[i]: i for i in range(1, len(status))}

    # Открытие файла на запись.
    with open(to_file_status_dict, 'w', encoding='utf-8') as to_file:
        to_file.write('ochered;last_step;status\n')
        # Итерация по ключам (очередям).
        for now in status_dict:
            # Итерация по спискам шагов.
            for step in status_dict[now].keys():
                # Выделение одного последнего шага из списка.
                for last_step in step.split(','):
                    # Запись в файл. Очередь, последний шаг, статус.
                    to_file.write(f'{now};{last_step};{my_status(now, last_step, status_dict, status)}\n')
