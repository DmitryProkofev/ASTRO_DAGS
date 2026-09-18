
#Есть словарь `{'a': 1, 'b': 2, 'c': 3}`. Поменяйте ключи и значения местами.  
def task_1(data_dict:dict) -> dict:
    dict_keys = data_dict.keys()
    new_data_dict = {}
    for el in dict_keys:
        new_data_dict[data_dict[el]] = el

    return new_data_dict

#  Как удалить дубликаты из списка, сохранив порядок?  [1, 2, 3, 3, 4, 5, 6, 6, 6]

# наивный
def task_2(data:list) -> list:
    new_data = []
    for el in data:
        if el not in new_data:
            new_data.append(el)

    return new_data

# через словарь
def task_3(data:list) -> list:
    d = {}
    for el in data:
        if d.get(el, 1) == 1:
            d[el] = 0
    return d.keys()


#  Как получить только дубликаты?  [1, 2, 3, 3, 4, 5, 6, 6, 6]
def task_4(data:list) -> list:
    d = {}
    result = []
    for el in data:
        if d.get(el, False):
            result.append(el)
        else:
            d[el] = 1

    return result
