import json
import sqlite3
from plistlib import loads
from peewee import SqliteDatabase, IntegerField, Model, fn

database = SqliteDatabase('sales.db')

class ModelBase(Model):
    class Meta:
        database = database


class Sales(ModelBase):
    order_id = IntegerField()
    customer_id = IntegerField()
    amount = IntegerField()


def create_tables():
    database.create_tables([Sales])

def load_data():
    with open("data.json", "r") as f:
        data = json.load(f)

    conn = sqlite3.connect("sales.db")
    cursor = conn.cursor()

    with database.atomic():
        for item in data:
            Sales.create(
                order_id=item["order_id"],
                customer_id=item["customer_id"],
                amount=item["amount"]
            )

def show_sum_amount():
    query = Sales.select(Sales.order_id,
                         fn.SUM(Sales.amount).alias("sales_amount")).group_by(Sales.order_id)
    for q in query:
        print(q.order_id, q.sales_amount)

if __name__ == "__main__":
    # create_tables()
    # load_data()
    show_sum_amount()



