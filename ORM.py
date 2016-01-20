import mysql.connector
import csv


##################
## File Manager ##
##################
class FileManager:
    @staticmethod
    def get_file_rows(filename):
        rows = []
        with open(filename, "r", encoding="utf8") as f:
            for line in f:
                rows.append(line)
        return rows

    @staticmethod
    def execute_sql_commands(command):
        connect_to_db = mysql.connector.connect(user='northwind_admin', password='admin',host='127.0.0.1',database='northwind')
        cursor = connect_to_db.cursor()
        try:
            cursor.execute(command)
            connect_to_db.commit()
        except mysql.connector.Error as error:
            print(error)
            connect_to_db.rollback()
            connect_to_db.close()


############################
## Employee table Manager ##
############################
class Employees:
    def to_csv(self):
        objs = [self.EmployeeID,self.LastName,self.FirstName,self.Title,self.TitleOfCourtesy,
                self.BirthDate,self.HireDate,self.Address,self.City,self.Region,self.PostalCode,
                self.Country,self.HomePhone,self.Extension,self.Photo,self.Notes,self.ReportsTo,self.PhotoPath,self.Salary]
        return "; ".join(objs)

    def persist(self):
        sql = "INSERT INTO employees_clone VALUES({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}',{16},'{17}',{18})".format(self.EmployeeID,self.LastName,self.FirstName,self.Title,self.TitleOfCourtesy,
              self.BirthDate,self.HireDate,self.Address,self.City,self.Region,self.PostalCode,self.Country,self.HomePhone,self.Extension,self.Photo,self.Notes,self.ReportsTo,self.PhotoPath,self.Salary)
        FileManager.execute_sql_commands(sql)

    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        EMPLOYEE = Employees()
        EMPLOYEE.EmployeeID = parsed_row[0]
        EMPLOYEE.LastName = parsed_row[1]
        EMPLOYEE.FirstName = parsed_row[2]
        EMPLOYEE.Title = parsed_row[3]
        EMPLOYEE.TitleOfCourtesy = parsed_row[4]
        EMPLOYEE.BirthDate = parsed_row[5]
        EMPLOYEE.HireDate = parsed_row[6]
        EMPLOYEE.Address = parsed_row[7]
        EMPLOYEE.City = parsed_row[8]
        EMPLOYEE.Region = parsed_row[9]
        EMPLOYEE.PostalCode = parsed_row[10]
        EMPLOYEE.Country = parsed_row[11]
        EMPLOYEE.HomePhone = parsed_row[12]
        EMPLOYEE.Extension = parsed_row[13]
        EMPLOYEE.Photo = parsed_row[14]
        EMPLOYEE.Notes = parsed_row[15]
        EMPLOYEE.ReportsTo = parsed_row[16]
        EMPLOYEE.PhotoPath = parsed_row[17]
        EMPLOYEE.Salary = parsed_row[18]
        return EMPLOYEE

    def get_data(self):
        datas = FileManager.get_file_rows("employees.csv")
        for i in range(1, len(datas)):
            employee = Employees.parse(datas[i])
            employee.persist()


emp = Employees()
emp.to_csv()


############################
## Customer table Manager ##
############################
class Customers:
    def to_csv(self):
            try:
                self.connect_to_db = mysql.connector.connect(user='northwind_admin', password='admin',host='127.0.0.1',database='northwind')
                self.cursor = self.connect_to_db.cursor()
                self.cursor.execute("SELECT * FROM customers")
                self.rows = self.cursor.fetchall()
                self.fp = open('customers.csv', 'a',newline='',encoding='utf-8')
                self.myFile = csv.writer(self.fp)
                self.myFile.writerows(self.rows)
                self.connect_to_db.commit()
                self.fp.close()
            except Exception as error:
                self.connect_to_db.rollback()
                print(error)

    def persist(self):
        sql = "INSERT INTO customers VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')".format(self.CustomerID,self.CompanyName,self.ContactName,self.ContactTitle,self.Address,self.City,
              self.Region,self.PostalCode,self.Country,self.Phone,self.Fax)
        FileManager.execute_sql_commands(sql)

    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        CUSTOMERS = Customers()
        CUSTOMERS.CustomerID = parsed_row[0]
        CUSTOMERS.CompanyName = parsed_row[1]
        CUSTOMERS.ContactName = parsed_row[2]
        CUSTOMERS.ContactTitle = parsed_row[3]
        CUSTOMERS.Address = parsed_row[4]
        CUSTOMERS.City = parsed_row[5]
        CUSTOMERS.Region = parsed_row[6]
        CUSTOMERS.PostalCode = parsed_row[7]
        CUSTOMERS.Country = parsed_row[8]
        CUSTOMERS.Phone = parsed_row[9]
        CUSTOMERS.Fax = parsed_row[10]
        return CUSTOMERS


    def get_data(self):
        datas = FileManager.get_file_rows("customers.csv")
        for i in range(1, len(datas)):
            CUSTOMERS = Customers.parse(datas[i])
            CUSTOMERS.persist()

# C = Customers()
# C.get_data()


################################
## OrderDetails table Manager ##
################################
class OrderDetails:
    def to_csv(self):
            try:
                self.connect_to_db = mysql.connector.connect(user='northwind_admin', password='admin',host='127.0.0.1',database='northwind')
                self.cursor = self.connect_to_db.cursor()
                self.cursor.execute("SELECT * FROM orderdetails")
                self.rows = self.cursor.fetchall()
                self.fp = open('order_datails.csv', 'a',newline='',encoding='utf-8')
                self.myFile = csv.writer(self.fp)
                self.myFile.writerows(self.rows)
                self.connect_to_db.commit()
                self.fp.close()
            except Exception as error:
                self.connect_to_db.rollback()
                print(error)

    def persist(self):
        sql = "INSERT INTO orderdetails VALUES({0},{1},{2},{3},{4})".format(self.OrderID,self.ProductID,self.UnitPrice,self.Quantity,self.Discount)
        FileManager.execute_sql_commands(sql)


    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        ORDERDETAILS = OrderDetails()
        ORDERDETAILS.OrderID = parsed_row[0]
        ORDERDETAILS.ProductID = parsed_row[1]
        ORDERDETAILS.UnitPrice = parsed_row[2]
        ORDERDETAILS.Quantity = parsed_row[3]
        ORDERDETAILS.Discount = parsed_row[4]
        return ORDERDETAILS

    def get_data(self):
        datas = FileManager.get_file_rows("order_datails.csv")
        for i in range(1, len(datas)):
            ORDERDETAILS = OrderDetails.parse(datas[i])
            ORDERDETAILS.persist()

# ORDER_D = OrderDetails()
# ORDER_D.to_csv()


#########################
## Order table Manager ##
#########################
class Orders:

    def to_csv(self):
        pass

    def persist(self):
        sql = "INSERT INTO orderdetails VALUES({0},{1},{2},{3},{4})".format(self.OrderID,self.ProductID,self.UnitPrice,self.Quantity,self.Discount)
        FileManager.execute_sql_commands(sql)

    @staticmethod
    def parse(row):
        parsed_row = row.split(";")
        ORDER = OrderDetails()
        ORDER.OrderID = parsed_row[0]
        ORDER.CustomerID = parsed_row[1]
        ORDER.EmployeeID = parsed_row[2]
        ORDER.ContactTitle = parsed_row[3]
        ORDER.OrderDate = parsed_row[4]
        ORDER.RequiredDate = parsed_row[5]
        ORDER.ShippedDate = parsed_row[6]
        ORDER.ShipVia = parsed_row[7]
        ORDER.Freight = parsed_row[8]
        ORDER.ShipName = parsed_row[9]
        ORDER.ShipAddress = parsed_row[10]
        ORDER.ShipCity = parsed_row[11]
        ORDER.ShipRegion = parsed_row[12]
        ORDER.ShipPostalCode = parsed_row[13]
        ORDER.ShipCountry = parsed_row[14]
        return ORDER


    def get_data(self):
        datas = FileManager.get_file_rows("orders.csv")
        for i in range(1, len(datas)):
            ORDER = Orders.parse(datas[i])
            ORDER.persist()