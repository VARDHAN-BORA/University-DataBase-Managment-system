from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import pandas as pd
from flask import Flask, jsonify, request


# Flask application configuration
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///university_schema.db'  # SQLite database
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SQLAlchemy
db = SQLAlchemy(app)



def paginate(query, page, page_size):
    """Paginate the query results."""
    total_records = query.count()
    records = query.offset((page - 1) * page_size).limit(page_size).all()
    return records, total_records

# -------------------------- DATABASE MODELS --------------------------

# Department model
class Department(db.Model):
    dept_name = db.Column(db.String, primary_key=True)  # Primary key
    building = db.Column(db.String)
    budget = db.Column(db.Float)
    instructors = db.relationship('Instructor', backref='department', lazy=True)
    students = db.relationship('Student', backref='department', lazy=True)
    courses = db.relationship('Course', backref='department', lazy=True)

# Instructor model
class Instructor(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Primary key
    name = db.Column(db.String(100), nullable=False)
    dept_name = db.Column(db.String, db.ForeignKey("department.dept_name"), nullable=False)  # Foreign key
    salary = db.Column(db.Float, nullable=False)
    teaches = db.relationship('Teaches', backref='instructor', lazy=True)
    advisors = db.relationship('Advisor', backref='instructor', lazy=True)

# Student model
class Student(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Primary key
    name = db.Column(db.String(100), nullable=False)
    dept_name = db.Column(db.String, db.ForeignKey("department.dept_name"), nullable=False)  # Foreign key
    tot_cred = db.Column(db.Integer)
    takes = db.relationship('Takes', backref='student', lazy=True)
    advisors = db.relationship('Advisor', backref='student', lazy=True)

# Course model
class Course(db.Model):
    course_id = db.Column(db.String, primary_key=True)  # Primary key
    title = db.Column(db.String, nullable=False)
    dept_name = db.Column(db.String, db.ForeignKey("department.dept_name"), nullable=False)  # Foreign key
    credits = db.Column(db.Integer, nullable=False)
    sections = db.relationship('Section', backref='course', lazy=True)
    prereqs = db.relationship('Prereq', backref='course', lazy=True)

# Section model
class Section(db.Model):
    course_id = db.Column(db.String, db.ForeignKey("course.course_id"), primary_key=True)  # Composite key
    sec_id = db.Column(db.Integer, primary_key=True)
    semester = db.Column(db.String, primary_key=True)
    year = db.Column(db.Integer, primary_key=True)
    building = db.Column(db.String)
    room_no = db.Column(db.String)
    time_slot_id = db.Column(db.String, db.ForeignKey("time_slot.time_slot_id"))
    takes = db.relationship('Takes', backref='section', lazy=True)
    teaches = db.relationship('Teaches', backref='section', lazy=True)

# TimeSlot model
class TimeSlot(db.Model):
    time_slot_id = db.Column(db.String, primary_key=True)  # Primary key
    day = db.Column(db.String)
    start_time = db.Column(db.Time)
    end_time = db.Column(db.Time)
    sections = db.relationship('Section', backref='time_slot', lazy=True)

# Takes model (many-to-many between Student and Section)
class Takes(db.Model):
    student_id = db.Column(db.Integer, db.ForeignKey("student.id"), primary_key=True)  # Composite key
    course_id = db.Column(db.String, db.ForeignKey("section.course_id"), primary_key=True)
    sec_id = db.Column(db.Integer, primary_key=True)
    semester = db.Column(db.String, primary_key=True)
    year = db.Column(db.Integer, primary_key=True)
    grade = db.Column(db.String)

# Teaches model (many-to-many between Instructor and Section)
class Teaches(db.Model):
    instructor_id = db.Column(db.Integer, db.ForeignKey("instructor.id"), primary_key=True)  # Composite key
    course_id = db.Column(db.String, db.ForeignKey("section.course_id"), primary_key=True)
    sec_id = db.Column(db.Integer, primary_key=True)
    semester = db.Column(db.String, primary_key=True)
    year = db.Column(db.Integer, primary_key=True)

# Classroom model
class Classroom(db.Model):
    building = db.Column(db.String, primary_key=True)  # Composite key
    room_no = db.Column(db.String, primary_key=True)
    capacity = db.Column(db.Integer)

# Prerequisite model
class Prereq(db.Model):
    course_id = db.Column(db.String, db.ForeignKey("course.course_id"), primary_key=True)  # Composite key
    prereq_id = db.Column(db.String, primary_key=True)

# Advisor model
class Advisor(db.Model):
    s_id = db.Column(db.Integer, db.ForeignKey("student.id"), primary_key=True)  # Composite key
    i_id = db.Column(db.Integer, db.ForeignKey("instructor.id"), primary_key=True)

    def __repr__(self):
        return f"<Advisor Student ID: {self.s_id}, Instructor ID: {self.i_id}>"

# -------------------------- API ENDPOINTS --------------------------

# API Endpoints
@app.route('/')
def home():
    return "Welcome to the University Database API!"

@app.route('/departments', methods=['GET'])
def get_departments():
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        
        # Fetch paginated data
        departments_query = Department.query
        departments, total_records = paginate(departments_query, page, page_size)
        
        # Build response
        response = [
            {
                "dept_name": dept.dept_name,
                "building": dept.building,
                "budget": dept.budget,
                "instructors": [
                    {"id": ins.id, "name": ins.name, "salary": ins.salary}
                    for ins in dept.instructors
                ],
                "students": [
                    {"id": stu.id, "name": stu.name}
                    for stu in dept.students
                ],
            }
            for dept in departments
        ]
        
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": total_records,
                "page": page,
                "page_size": page_size,
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "error": str(e)
        })


@app.route('/students', methods=['GET'])
def get_students():
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        
        # Fetch paginated data
        students_query = Student.query
        students, total_records = paginate(students_query, page, page_size)
        
        # Build response
        response = [
            {
                "id": stu.id,
                "name": stu.name,
                "dept_name": stu.dept_name,
                "tot_cred": stu.tot_cred,
                "courses": [
                    {
                        "course_id": take.course_id,
                        "section_id": take.sec_id,
                        "semester": take.semester,
                        "year": take.year,
                    }
                    for take in stu.takes
                ],
            }
            for stu in students
        ]
        
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": total_records,
                "page": page,
                "page_size": page_size,
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "error": str(e)
        })

@app.route('/courses', methods=['GET'])
def get_courses():
    try:
        # Get and validate pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))

        # Fetch paginated data
        courses_query = Course.query
        total_records = courses_query.count()
        courses = courses_query.offset((page - 1) * page_size).limit(page_size).all()

        # Build response
        response = [
            {
                "course_id": course.course_id,
                "title": course.title,
                "dept_name": course.dept_name,
                "credits": course.credits,
                "instructors": [
                    {
                        "instructor_id": teach.instructor_id,
                        "name": teach.instructor.name
                    }
                    for section in course.sections for teach in section.teaches
                ]
            }
            for course in courses
        ]

        # Return paginated response
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": total_records,
                "page": page,
                "page_size": page_size,
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "error": str(e)
        })

@app.route('/time_slots', methods=['GET'])
def get_time_slots():
    try:
        time_slots = TimeSlot.query.all()
        response = [
            {
                "time_slot_id": time_slot.time_slot_id,
                "day": time_slot.day,
                "start_time": time_slot.start_time.strftime("%H:%M"),
                "end_time": time_slot.end_time.strftime("%H:%M"),
            }
            for time_slot in time_slots
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(time_slots),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })
@app.route('/instructors', methods=['GET'])
def get_instructors():
    try:
        instructors = Instructor.query.all()
        response = [
            {
                "id": instructor.id,
                "name": instructor.name,
                "dept_name": instructor.dept_name,
                "salary": instructor.salary,
                "courses": [
                    {
                        "course_id": teach.course_id,
                        "section": teach.sec_id,
                        "semester": teach.semester,
                        "year": teach.year,
                    }
                    for teach in instructor.teaches
                ],
            }
            for instructor in instructors
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(instructors),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })
        
@app.route('/advisors', methods=['GET'])
def get_advisors():
    try:
        advisors = Advisor.query.all()
        print(f"Advisors: {advisors}")  # Debugging print

        response = [
            {
                "student_id": advisor.s_id,
                "student_name": advisor.student.name if advisor.student else None,
                "instructor_id": advisor.i_id,
                "instructor_name": advisor.instructor.name if advisor.instructor else None,
            }
            for advisor in advisors
        ]
        print(f"Response: {response}")  # Debugging print

        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(advisors),
                "records": response
            }
        })
    except Exception as e:
        print(f"Error: {e}")  # Debugging print
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })


@app.route('/classrooms', methods=['GET'])
def get_classrooms():
    try:
        classrooms = Classroom.query.all()
        response = [
            {
                "building": classroom.building,
                "room_no": classroom.room_no,
                "capacity": classroom.capacity,
            }
            for classroom in classrooms
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(classrooms),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })
@app.route('/sections', methods=['GET'])
def get_sections():
    try:
        sections = Section.query.all()
        response = [
            {
                "course_id": section.course_id,
                "sec_id": section.sec_id,
                "semester": section.semester,
                "year": section.year,
                "building": section.building,
                "room_no": section.room_no,
                "time_slot": {
                    "time_slot_id": section.time_slot_id,
                } if section.time_slot else None,
            }
            for section in sections
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(sections),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })
@app.route('/prerequisites', methods=['GET'])
def get_prerequisites():
    try:
        prereqs = Prereq.query.all()
        response = [
            {
                "course_id": prereq.course_id,
                "prereq_id": prereq.prereq_id,
            }
            for prereq in prereqs
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(prereqs),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })

@app.route('/takes', methods=['GET'])
def get_takes():
    try:
        takes = Takes.query.all()
        response = [
            {
                "student_id": take.student_id,
                "course_id": take.course_id,
                "section_id": take.sec_id,
                "semester": take.semester,
                "year": take.year,
                "grade": take.grade
            }
            for take in takes
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(takes),
                "records": response
            }
        })
    except Exception as e:
        print(f"Error: {e}")  # Debugging print
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })

@app.route('/teaches', methods=['GET'])
def get_teaches():
    try:
        teaches_records = Teaches.query.all()
        response = [
            {
                "instructor_id": teach.instructor_id,
                "course_id": teach.course_id,
                "section_id": teach.sec_id,
                "semester": teach.semester,
                "year": teach.year,
            }
            for teach in teaches_records
        ]
        return jsonify({
            "code": 1,
            "msg": "Success",
            "data": {
                "total": len(teaches_records),
                "records": response
            }
        })
    except Exception as e:
        return jsonify({
            "code": 0,
            "msg": "Error",
            "data": {
                "total": 0,
                "records": []
            }
        })

# -------------------------- Loading DATA --------------------------


def load_data():
    try:
        # Load Departments
        print("Loading Departments...")
        departments_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/department.csv')
        for _, row in departments_df.iterrows():
            if not Department.query.filter_by(dept_name=row['dept_name']).first():
                db.session.add(Department(
                    dept_name=row['dept_name'],
                    building=row['building'],
                    budget=row['budget']
                ))
        db.session.commit()
        print("Departments loaded successfully!\n")

        # Load Students
        print("Loading Students...")
        students_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/student.csv')
        for _, row in students_df.iterrows():
            if not Student.query.filter_by(id=row['ID']).first():
                db.session.add(Student(
                    id=row['ID'],
                    name=row['name'],
                    dept_name=row['dept_name'],
                    tot_cred=row['tot_cred']
                ))
        db.session.commit()
        print("Students loaded successfully!\n")

        # Load Courses
        print("Loading Courses...")
        courses_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/course.csv')
        for _, row in courses_df.iterrows():
            if not Course.query.filter_by(course_id=row['course_id']).first():
                db.session.add(Course(
                    course_id=row['course_id'],
                    title=row['title'],
                    dept_name=row['dept_name'],
                    credits=row['credits']
                ))
        db.session.commit()
        print("Courses loaded successfully!\n")

        # Load Instructors
        print("Loading Instructors...")
        instructors_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/instructor.csv')
        for _, row in instructors_df.iterrows():
            if not Instructor.query.filter_by(id=row['ID']).first():
                db.session.add(Instructor(
                    id=row['ID'],
                    name=row['name'],
                    dept_name=row['dept_name'],
                    salary=row['salary']
                ))
        db.session.commit()
        print("Instructors loaded successfully!\n")


        print("Loading Advisors...")
        advisors_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/advisor.csv')
        for _, row in advisors_df.iterrows():
            if not Advisor.query.filter_by(s_id=int(row['s_id']), i_id=int(row['i_id'])).first():
                db.session.add(Advisor(
                    s_id=int(row['s_id']),
                    i_id=int(row['i_id'])
                ))
        db.session.commit()
        print("Advisors loaded successfully!\n")




        # Load Classrooms
        print("Loading Classrooms...")
        classrooms_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/classroom.csv')
        for _, row in classrooms_df.iterrows():
            if not Classroom.query.filter_by(building=row['building'], room_no=row['room_no']).first():
                db.session.add(Classroom(
                    building=row['building'],
                    room_no=row['room_no'],
                    capacity=row['capacity']
                ))
        db.session.commit()
        print("Classrooms loaded successfully!\n")

        # Load Prerequisites
        print("Loading Prerequisites...")
        prereqs_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/prereq.csv')
        for _, row in prereqs_df.iterrows():
            if not Prereq.query.filter_by(course_id=row['course_id'], prereq_id=row['prerq_id']).first():
                db.session.add(Prereq(
                    course_id=row['course_id'],
                    prereq_id=row['prerq_id']
                ))
        db.session.commit()
        print("Prerequisites loaded successfully!\n")

        # Load Sections
        print("Loading Sections...")
        sections_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/section.csv')
        for _, row in sections_df.iterrows():
            if not Section.query.filter_by(course_id=row['course_id'], sec_id=row['sec_id'], semester=row['semester'], year=row['year']).first():
                db.session.add(Section(
                    course_id=row['course_id'],
                    sec_id=row['sec_id'],
                    semester=row['semester'],
                    year=row['year'],
                    building=row['building'],
                    room_no=row['room_no'],
                    time_slot_id=row['time_slot_id']
                ))
        db.session.commit()
        print("Sections loaded successfully!\n")

        # Load Takes
        print("Loading Takes...")
        takes_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/takes.csv')
        for _, row in takes_df.iterrows():
            if not Takes.query.filter_by(student_id=row['ID'], course_id=row['course_id'], sec_id=row['sec_id'], semester=row['semester'], year=row['year']).first():
                db.session.add(Takes(
                    student_id=row['ID'],
                    course_id=row['course_id'],
                    sec_id=row['sec_id'],
                    semester=row['semester'],
                    year=row['year'],
                    grade=row['grade']
                ))
        db.session.commit()
        print("Takes loaded successfully!\n")

        # Load Teaches
        print("Loading Teaches...")
        teaches_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/teaches.csv')
        for _, row in teaches_df.iterrows():
            if not Teaches.query.filter_by(instructor_id=row['ID'], course_id=row['course_id'], sec_id=row['sec_id'], semester=row['semester'], year=row['year']).first():
                db.session.add(Teaches(
                    instructor_id=row['ID'],
                    course_id=row['course_id'],
                    sec_id=row['sec_id'],
                    semester=row['semester'],
                    year=row['year']
                ))
        db.session.commit()
        print("Teaches loaded successfully!\n")

        # Load Time Slots
        print("Loading Time Slots...")
        time_slots_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/time_slot.csv')
        for _, row in time_slots_df.iterrows():
            try:
                # Convert string times to Python time objects
                start_time = datetime.strptime(row['start_time'], "%I:%M").time()
                end_time = datetime.strptime(row['end_time'], "%I:%M").time()

                # Check if the record exists and insert if it doesn't
                if not TimeSlot.query.filter_by(time_slot_id=row['time_slot_id']).first():
                    db.session.add(TimeSlot(
                        time_slot_id=row['time_slot_id'],
                        day=row['day'],
                        start_time=start_time,
                        end_time=end_time
                    ))
            except Exception as e:
                print(f"Error processing time slot {row}: {e}")
        db.session.commit()
        print("Time Slots loaded successfully!\n")

        print("All data loaded successfully!")

    except Exception as e:
        print(f"Error in load_data: {e}")
        
        
        print("Loading Takes...")
        takes_df = pd.read_csv('/Users/venkatasaivardhanbora/Desktop/APIproject/takes.csv')
        for _, row in takes_df.iterrows():
            if not Takes.query.filter_by(
                student_id=row['ID'],
                course_id=row['course_id'],
                sec_id=row['sec_id'],
                semester=row['semester'],
                year=row['year']
            ).first():
                db.session.add(Takes(
                    student_id=row['ID'],
                    course_id=row['course_id'],
                    sec_id=row['sec_id'],
                    semester=row['semester'],
                    year=row['year'],
                    grade=row.get('grade')  # Handle missing grade values
                ))
        db.session.commit()
        print("Takes loaded successfully!\n")

# -------------------------- RUNNING APPLICATION --------------------------
if __name__ == '__main__':
    with app.app_context():  # Ensure app context is active
        db.drop_all()  # Reset database tables
        db.create_all()  # Create fresh tables
        load_data()  # Load data into the database
    app.run(debug=True)



