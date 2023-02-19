import sqlalchemy as db
print(f"SQLAlchemy version {db.__version__}")

engine = db.create_engine("sqlite:///blarvitz.sqlite")
conn = engine.connect()

metadata = db.MetaData()

Student = db.Table('Student', metadata,
                   db.Column('Id', db.Integer(), primary_key=True),
                   db.Column('Name', db.String(255), nullable=False),
                   db.Column('Major', db.String(255), default="Math"),
                   db.Column('Pass', db.Integer(), default=1)
                   )
metadata.create_all(engine)

query = db.insert(Student).values(Id=1, Name='Matthew', Major='English', Pass=1)
Result = conn.execute(query)

output = conn.execute(Student.select()).fetchall()
print(output)

query = db.insert(Student)
values_list = [{'Id': 2, 'Name': 'Nisha',   'Major': "Science", 'Pass': 0},
               {'Id': 3, 'Name': 'Natasha', 'Major': "Math",    'Pass': 1},
               {'Id': 4, 'Name': 'Ben',     'Major': "English", 'Pass': 0}
              ]
Result = conn.execute(query, values_list)

output = conn.execute(Student.select()).fetchall()
print(output)

output = conn.execute("SELECT * FROM Student")
print(output.fetchall())

query = Student.select().where(Student.columns.Major == 'English')
output = conn.execute(query)
print(output.fetchall())

query = Student.select().where(db.and_(Student.columns.Major == 'English', Student.columns.Pass != True))
output = conn.execute(query)
print(output.fetchall())

"""
in: Student.select().where(Student.columns.Major.in_(['English', 'Math']))
and, or, not: Student.select().where(db.or_(Student.columns.Major == 'English', Student.columns.Pass = True))
order by: Student.select().order_by(db.desc(Student.columns.Name))
limit: Student.select().limit(3)
sum, avg, count, min, max: db.select([db.func.sum(Student.columns.Id)])
group by: db.select([db.func.sum(Student.columns.Id), Student.columns.Major]).group_by(Student.columns.Pass)
distinct: db.select([Student.columns.Major.distinct()])
"""