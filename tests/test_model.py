from redisor.field import StringField, AutoIncrementField
from redisor.model import Model
from redisor import get_client


class Person(Model):
    database = get_client()
    pk = AutoIncrementField(name='pk', primary_key=True)
    name = StringField(name='name')
    address = StringField(name='address', default='1998')

person = Person(name=1, address="heiheihie")
print(Person.__dict__)
person.name = 123
person.name = 123
print(person._fields)
print(person.__dict__)
person.save()
person.update({"name": "xingshengyao"})
