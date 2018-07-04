import time

from redisor.field import StringField, AutoIncrementField, JsonField
from redisor.model import Model
from redisor import get_client


class Person(Model):
    __database__ = get_client()
    pk = AutoIncrementField(name='pk')
    name = StringField(name='name')
    address = StringField(name='address', default='1998')
    others = JsonField(name="others")
    new = StringField(name='new', default=time.time)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": self.address,
            "pk": self.pk,
            "others": self.others
        }

# person = Person(name=1, address="heiheihie")
# print(Person.__dict__)
# person.name = 123
# person.name = 123
# print(person._fields)
# print(person.__dict__)
# person.save()
# # person.update({"name": "xingshengyao"})
# person2 = Person(name=2, address="zxxxx")
# person2.save()
# person = Person(name='xiongda')
# person.others = {"age":18, "ni": "wo"}
# person.save()
# print(person.id)
# print(person.address)
# print(person.others)
new = Person.objects.create(name="xionger", address="zzz")
print(new.pk)
print(new.name)
print(new.__dict__)
