import json
import sqlite3

from peewee import Model, SqliteDatabase, CharField, IntegerField, ForeignKeyField, fn

database = SqliteDatabase('employee.db')

class BaseModel(Model):
    class Meta:
        database = database

class Department(BaseModel):
    departmentID = IntegerField()
    departmentName = CharField(max_length=32)


class Employee(BaseModel):
    employeeID = IntegerField()
    name = CharField(max_length=32)
    salary = IntegerField()
    departmentID = ForeignKeyField(Department, backref="department")
    JobTitle = CharField(max_length=32)


def create_tables():
    database.create_tables([Department, Employee])

def load():
    with open(f"department.json", "r") as f:
        departments_data = json.load(f)

    with open(f"employee.json", "r") as f:
        employees_data = json.load(f)

    conn = sqlite3.connect("employee.db")
    cursor = conn.cursor()

    for dept in departments_data:
        Department.get_or_create(
            departmentID=dept["departmentID"],
            defaults={"departmentName": dept["departmentName"]}
        )

    for emp in employees_data:
        Employee.get_or_create(
            employeeID=emp["employeeID"],
            defaults={
                "name": emp["name"],
                "salary": emp["salary"],
                "departmentID": emp["departmentID"],
                "JobTitle": emp["JobTitle"]
            }
        )

def show_id():
    largest_dept = (Department.select(Department, fn.COUNT(Employee.employeeID).alias("num_employess")).join(Employee)
                    .group_by(Employee).order_by(fn.COUNT(Employee.employeeID).desc()).limit(1).get())

    employees_in_largest_dept = Employee.select().where(Employee.departmentID == largest_dept)

    for emp in employees_in_largest_dept:
        print(f"{emp.name} - {emp.JobTitle} - {emp.departmentID.departmentName}")


def show_name():
    query = (Employee.select(Employee, Department).join(Department))

    for q in query:
        print(q.name, "->", "Department ID" , q.departmentID)

if __name__ == "__main__":
    # create_tables()
    # load()
    # show_id()
    show_name()




