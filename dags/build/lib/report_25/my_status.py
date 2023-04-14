# Функция для отображения статуса. Необходимо передать очередь и последний шаг.


def my_status(ochered, key, status_dict, status):
    for step_now in status_dict[ochered].keys():
        if key in step_now.split(','):
            return status[status_dict[ochered][step_now]]
