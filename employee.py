from flask import Flask, jsonify, request, render_template, redirect
from flask_mysqldb import MySQL
import yaml
from datetime import date
from datetime import datetime

app = Flask(__name__)

db = yaml.safe_load(open('employeeDB.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASS'] = db['mysql_pass']
app.config['MYSQL_DB'] = db['mysql_db']

mysql = MySQL(app)

@app.route('/')
def index():
    return "Welcome to Employee API"

@app.route('/app/v1/employee/addEmployee',methods=['POST'])
def add_employee():
    name = request.json['name']
    department = request.json['department']
    date_of_birth = request.json['date_of_birth']
    date_of_joining = request.json['date_of_joining']
    today = datetime.now()
    employee_id = department +'_'+ datetime.strptime(date_of_joining, "%d-%m-%Y").strftime("%Y%m%d") +'_'+str(today.hour)+str(today.minute)+str(today.second)
    getQuery = "select * from employee where employee_id = '"+employee_id+"' and is_deleted = false"
    cur = mysql.connection.cursor()
    res = cur.execute(getQuery)
    today = datetime.now()
    d1 = today.strftime("%Y-%m-%d %H:%M:%S")
    insertQuery = "insert into employee (name,employee_id,department,date_of_birth, date_of_joining, created_at, is_deleted, is_updated, version_number) values ('"+name+"','"+employee_id+"','"+department+"','"+date_of_birth+"','"+date_of_joining+"','"+d1+"','"+str(0)+"','"+str(1)+"','"+str(1)+"')"
    cur.execute(insertQuery)
    mysql.connection.commit()
    getQuery = "select * from employee where employee_id = '"+employee_id+"' and is_deleted = 0 and is_updated = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    mysql.connection.commit()
    cur.close()
    return jsonify("Employee added successfully"),200


@app.route('/app/v1/employee/getUpdatedEmployee',methods=['GET'])
def get_employee_by_employee_id_updated():
    argsValue = request.args.get('employee_id')
    getQuery = "select * from employee where employee_id = '"+argsValue+"' and is_deleted = 0 and is_updated = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    mysql.connection.commit()
    cur.close()
    return jsonify(json_data),200


@app.route('/app/v1/employee/getAllUpdatedEmployees',methods=['GET'])
def get_all_employee_updated():
    # argsphone = request.args.get('employee_id')
    getQuery = "select * from employee where is_deleted = 0 and is_updated = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    mysql.connection.commit()
    cur.close()
    return jsonify(json_data),200


@app.route('/app/v1/employee/getAllDeletedEmployees',methods=['GET'])
def get_all_employee_deleted():
    # argsphone = request.args.get('employee_id')
    getQuery = "select * from employee where is_deleted = 1 "
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    mysql.connection.commit()
    cur.close()
    return jsonify(json_data),200


@app.route('/app/v1/employee/getDeletedEmployee',methods=['GET'])
def get_employee_by_employee_id_deleted():
    argsValue = request.args.get('employee_id')
    getQuery = "select * from employee where employee_id = '"+argsValue+"' and is_deleted = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    mysql.connection.commit()
    cur.close()
    return jsonify(json_data),200


@app.route('/app/v1/employee/updateEmployee',methods=['PUT'])
def update_employee_by_employee_id():
    argsValue = request.args.get('employee_id')
    getQuery = "select * from employee where employee_id = '"+argsValue+"' and is_deleted = 0 and is_updated = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    # row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        updateQuery = "update employee set is_updated = 0, is_deleted = 1 where employee_id = '"+argsValue+"' and is_deleted = 0 "
        cur = mysql.connection.cursor()
        cur.execute(updateQuery)

        name = result[2]
        employee_id = result[1]
        department = result[3]
        date_of_birth = result[4]
        date_of_joining = result[5]
        version_number = result[6]
        if 'name' in request.json:
            name = request.json['name']
        if 'date_of_birth' in request.json:
            date_of_birth = request.json['date_of_birth']
        if 'date_of_joining' in request.json:
            date_of_joining = request.json['date_of_joining']

        today = datetime.now()
        d1 = today.strftime("%Y-%m-%d %H:%M:%S")
        createQuery = "insert into employee (name,employee_id,department,date_of_birth, date_of_joining, created_at, is_deleted, is_updated, version_number) values ('"+name+"','"+employee_id+"','"+department+"','"+date_of_birth+"','"+date_of_joining+"','"+d1+"','"+str(0)+"','"+str(1)+"','"+str(version_number+1)+"')"
        cur = mysql.connection.cursor()
        cur.execute(createQuery)

    mysql.connection.commit()
    cur.close()
    return jsonify("Employee updated successfully"),200




@app.route('/app/v1/employee/deleteEmployee',methods=['DELETE'])
def delete_employee():
    argsValue = request.args.get('employee_id')
    getQuery = "select * from employee where employee_id = '"+argsValue+"' and is_deleted = 0 and is_updated = 1"
    cur = mysql.connection.cursor()
    cur.execute(getQuery)
    # row_headers=[x[0] for x in cur.description]
    rv = cur.fetchall()
    for result in rv:
        updateQuery = "update employee set is_deleted = 1 where employee_id = '"+argsValue+"' and is_deleted = 0 "
        cur = mysql.connection.cursor()
        cur.execute(updateQuery)
    mysql.connection.commit()
    cur.close()
    return jsonify("Employee deleted successfully"),200
        

if __name__ == '__main__':
    app.run(debug=True)