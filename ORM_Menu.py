import msvcrt
from ORM import *
import os


menu_choosen=['1','2','3','4','5']
def menu():
    print("\n\n#File Manager Functions#")
    print("\t1. Create")
    print("\t2. Insert")
    print("\t3. Query")
    print("\t4. Drop")
    print("\t5. Exit")
    number = msvcrt.getwch()
    if number not in menu_choosen:
        menu()
    else:
        menu_choose(number)

def menu_choose(number):
    if number == '1':
        os.system('cls')
        print("Create db:")
        print()
        SQL_manager.create_schema()
    elif number == '2':
        os.system('cls')
        print("Insert to db:")
        print()
        SQL_manager.insert_data_to_tables()
    elif number == '3':
        os.system('cls')
        print("Query from db:")
        print()
        SQL_manager.list_meetups_for_a_particular_user()
        SQL_manager.list_meetups_which_after_date()
        SQL_manager.list_users_who_have_introduction()
    elif number == '4':
        os.system('cls')
        print("Drop all table from db:")
        print()
        SQL_manager.drop_all_table_from_database()
    elif number == '5':
        exit()