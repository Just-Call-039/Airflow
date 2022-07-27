# Функция обработки пользователей для отчета №10.


def clear_file(my_file):
    import time
    import re
    from commons.my_clear import my_c

    start_clear = time.time()
    step = 0
    to_file = my_file.replace('.csv', '_clear.csv')
    print(f"Создан файл: {to_file}.")

    # Открытие файла на запись.
    with open(to_file, 'w', encoding='utf-8') as to_file:
        # Открытие файла на чтение.
        with open(my_file, encoding='utf-8') as file:
            for now in file:

                step += 1
                # if step == 100:
                #     break

                # Пробегаем по каждой строке. Делим по ",".
                now = now.strip().split(';')
                # Записываем заголовок.
                if now[0] == 'id':
                    to_file.write('id;first_name;last_name;group;department_c;status\n')
                    continue
                # ИД.
                my_id = my_c(now[0])
                # Имя.
                first = now[1]
                # Фамилия.
                last = my_c(now[2])
                # Отдел.
                dep = my_c(now[3])
                # Группа.
                group = []
                # print(my_id, first, last, dep)

                # Если "я" стоит в начале имени, сотрудник уволен.
                if re.search(r'^я', first):
                    # Статус - уволен.
                    status = 'dismissed'
                    # Делим строку с именем по пробелу и "_".
                    first = re.split(r'[ _-]', first)
                    # Пробегаем по каждому слову в имени.
                    for word in first:
                        # И по каждой букве в слове.
                        for now in word:
                            # Если слово начинается с "я", просто пропускаем.
                            if now == 'я':
                                continue
                            # Если буква - это цифра, добавляем к группе сотрудника.
                            elif now.isdigit():
                                group.append(now)
                            # Если буква - заглавная, то это начало имени.
                            elif now.isupper():
                                # Ищем позицию заглавной буквы в слове. Делаем срез по строке.
                                k = word.find(now)
                                name = word[k:]
                                break
                    # Преобразую группу из списка в строку.
                    group = my_c(''.join(group))
                    # Если в строке с именем не было группы, то такой сотрудник записывается соответствующим образом.
                    if group == '':
                        group = 'unknown_group'
                # Если "я" нет в начале имени, сотрудник работает.
                else:
                    # Статус - работает.
                    status = 'working'
                    # Делим строку с именем по пробелу и "_".
                    first = re.split(r'[ _-]', first)
                    # Если длина строки == 1, в строке содержится только имя. Группа отсутствует.
                    if len(first) == 1:
                        name = my_c(first[0])
                        group = 'unknown_group'
                    # Иначе, извлекаем имя и группу.
                    else:
                        name = my_c(first[-1])
                        group = my_c(first[-2])

                # Проверка значений на пустые данные.
                if my_id is None or my_id == '' or my_id == ' ':
                    my_id = 'unknown_id'
                if name is None or name == '' or name == ' ':
                    name = 'unknown_name'
                if dep is None or dep == '' or dep == ' ':
                    dep = 'unknown_department'
                if status is None or status == '' or status == ' ':
                    status = 'unknown_status'

                # to_file.write('id,first_name,last_name,group,department_c,status')
                to_file.write(f'{my_id};{name};{last};{group};{dep};{status}\n')
                # print(my_id, name, last, group, dep, status)
    print(f"Время обработки {step} строк составило: {round(time.time() - start_clear, 3)} сек.")
