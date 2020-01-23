from pprint import pprint
from datetime import datetime

import pymongo


# 1. Создать мета-класс, определяющий в переменную класса COLLECTION имя коллекции,
#    с которой работает создаваемый класс. Имя коллекции должно генерироваться
#    автоматически и соответствовать имени класса, написанного с большой буквы.
class MyMetaClass(type):

    def __new__(cls, name, bases, dct):
        dct['COLLECTION'] = name.capitalize()
        new_class = super(MyMetaClass, cls).__new__(cls, name, bases, dct)
        return new_class
 

# 2. На основе созданного мета-класса создать класс с именем 'Account'.
#    При этом экземпляр такого класса имеет обязательный
#    строковый атрибут 'number' длиной ровно 13 символов.
class Account(metaclass=MyMetaClass):

    def __init__(self, number:str):
        if len(number) != 13:
            raise ValueError("The length of number must be equal to 13.")
        self.number = number

# print(Account.COLLECTION) # output: "Account"
# print(Account.number) # output: "0000000000000"


# 3. Необходимо написать агрегационный запрос, 
# который по каждому пользователю выведет последнее действие
# и общее количество для каждого из типов 'actions'.
def request_to_account():
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.test
    coll = db.account

    result = coll.aggregate([
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.actions"},
        {"$group": {
            "_id": {"number": "$number", "type": "$sessions.actions.type"},
            "count": {"$sum": 1},
            "created_at": {"$push": "$sessions.actions.created_at"}
        }},
        {"$group": {
            "_id": "$_id.number", 
            "actions": {"$addToSet": {
                "type": "$_id.type", 
                "count": "$count",
                "last": {"$max": "$created_at"}
            }}
        }}
    ])
    for res in result:
        pprint(res)  


# 4. Необходимо написать функцию, которая сделает запрос к платежам
# и найдёт для каждого платежа долг, который будет им оплачен.
# Платёж может оплатить только долг, имеющий более раннюю дату.
# Один платёж может оплатить только один долг, и каждый долг
# может быть оплачен только одним платежом. Платёж приоритетно должен
# выбрать долг с совпадающим месяцем (поле month). Если такого нет, 
# то самый старый по дате (поле date) долг.
# Результатом должна быть таблица найденных соответствий, 
# а также список платежей, которые не нашли себе долг.
def payments_to_accruals():
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.test

    coll_accrual = db.accrual
    coll_payment = db.payment
    result_accrual = list(coll_accrual.find())
    result_payment = list(coll_payment.find())

    # Поиск платежей и составление пар индентичных по месяцу.
    pairs = []
    payments_without_accurals = []
    i = 0
    while i < len(result_payment):
        j = 0
        while j < len(result_accrual):
            if (result_accrual[j]['month'] == result_payment[i]['month'] and
                result_accrual[j]['date'] < result_payment[i]['date']):
                pairs.append([result_accrual[j], result_payment[i]])
                del result_accrual[j]
                del result_payment[i]
                i -= 1
                break
            j += 1
        i += 1
    
    # Составление пар с различными месяцами.
    if len(result_accrual) < len(result_payment):
        x = len(result_accrual)
    else:
        x = len(result_payment)
    min_accr = {}
    while x:
        min_accr['date'] = datetime(2900, 1, 1)
        for accr in result_accrual:
            if accr['date'] < min_accr['date']:
                min_accr = accr
        if result_payment[x-1]['date'] > min_accr['date']:
            pairs.append([min_accr, result_payment[x-1]])
        del result_payment[x-1]
        x -= 1
    
    # Вывод результатов
    for x in pairs:
        print(x, '\n')
        
    if result_payment:
        print('Payments without accruals: ')
        print(result_payment)
    else:
        print('They are no payments.')


if __name__ == '__main__':
    # request_to_account()
    payments_to_accruals()
