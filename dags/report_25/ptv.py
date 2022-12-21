# Функция для проверки технической возможности.
# Необходимо передать очередь, шаги.


def teh_v(ochered, route, est_tehv, net_tehv):
    net_tv_step = ['105', '106', '107']
    route = tuple(i.strip() for i in str(route).split(','))
    ochered = str(ochered)
    # Если последний шаг в списке шагов с НТВ, то возвращаем 0.
    if route[-1] in net_tv_step:
        t_v = 0
        return t_v
    # Если последний шаг не в списке шагов с НТВ, то начинаем перебор шагов.
    elif route[-1] not in net_tv_step:
        for st in route:
            # Если очередь в словаре ЕТВ и найден соответствующий шаг для такой очереди, то возвращаем 1.
            if ochered in tuple(est_tehv.keys()) and st in est_tehv[ochered]:
                t_v = 1
                return t_v
            # Если очередь в словаре НТВ и найден соответствующий шаг для такой очереди, то возвращаем 0.
            elif ochered in tuple(net_tehv.keys()) and st in net_tehv[ochered]:
                t_v = 0
                return t_v
        # Если никакие условия не выполнены, то возвращаем "Didn't check".
        else:
            t_v = "Didn't check"
            return t_v
