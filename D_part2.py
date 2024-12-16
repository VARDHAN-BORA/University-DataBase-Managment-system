import requests
import redis
from pymongo import MongoClient

# Constants
BASE_URL = "http://127.0.0.1:5000"  #  API URL
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'university'


# Fetch data from APIs
def fetch_data(api_endpoint):
    data = []
    page = 1
    while True:
        response = requests.get(f"{BASE_URL}/{api_endpoint}", params={"page": page, "page_size": 10})
        if response.status_code != 200:
            print(f"Error fetching {api_endpoint} data")
            break
        records = response.json().get("data", {}).get("records", [])
        if not records:
            break
        data.extend(records)
        page += 1
    return data

# ----------------Store data in Redis------------------------------
def store_in_redis(redis_client, data, folder):
    for record in data:
        
        if folder == "students":
            key_name = record.get("id")
        elif folder == "courses":
            key_name = record.get("course_id")
        elif folder == "departments":
            key_name = record.get("dept_name")
        else:
            continue

        if key_name:
            redis_client.set(f"{folder}:{key_name}", str(record))


# ------------------------Store data in MongoDB------------------------------
def store_in_mongodb(db, data, collection_name):
    collection = db[collection_name]
    collection.insert_many(data)


#---------------------------- Query Redis---------------------------
def query_redis(redis_client):
    comp_sci_data = redis_client.get("departments:CompSci")
    peter_lynch_data = redis_client.get("students:1999")
    data_engineering_course = redis_client.get("courses:Data1050")

    print("\n--- Redis Query Results ---")
    print("Computer Science Department:", comp_sci_data)
    print("Peter Lynch Data:", peter_lynch_data)
    print("Data Engineering Course:", data_engineering_course)


# ---------Query MongoDB-----------------
def query_mongodb(db):
    dept_count = db.departments.count_documents({})
    cs_instructors = db.departments.find_one({"dept_name": "CompSci"})["instructors"]

    student_count = db.students.count_documents({})
    peter_courses = db.students.find_one({"name": "Peter Lynch"})["courses"]

    course_count = db.courses.count_documents({})
    data_science_course = db.courses.find_one({"title": "Hands-on data science"})

    print("\n--- MongoDB Query Results ---")
    print("Number of Departments:", dept_count)
    print("Instructors in CompSci Department:", cs_instructors)
    print("Number of Students:", student_count)
    print("Courses Peter Lynch Took:", peter_courses)
    print("Number of Courses:", course_count)
    print("Details of 'Hands-on Data Science' Course:", data_science_course)


# -------------Main Execution---------------------------
if __name__ == "__main__":
    # Fetch data from APIs
    departments_data = fetch_data("departments")
    students_data = fetch_data("students")
    courses_data = fetch_data("courses")

    # Store data in Redis
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    store_in_redis(redis_client, departments_data, "departments")
    store_in_redis(redis_client, students_data, "students")
    store_in_redis(redis_client, courses_data, "courses")
    print("Data successfully stored in Redis!")

    # Store data in MongoDB
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DATABASE_NAME]
    store_in_mongodb(db, departments_data, "departments")
    store_in_mongodb(db, students_data, "students")
    store_in_mongodb(db, courses_data, "courses")
    print("Data successfully stored in MongoDB!")

    # Query Redis
    query_redis(redis_client)

    # Query MongoDB
    query_mongodb(db)
