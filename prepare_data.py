import random
import MySQLdb
import os


conn = MySQLdb.connect(read_default_file = os.path.abspath('.') + '/mysql.cnf',
                       local_infile = 1,
                       sql_mode="ALLOW_INVALID_DATES",
                       db='Allegro') #<---- Change name of data base you wont to use !!!

c = conn.cursor()
c.execute("DROP TABLE IF EXISTS prepared_customers")
#----------Preparing data-------------------
c.execute("""UPDATE customers
            SET first_name = TRIM(LOWER(first_name)),
                last_name = TRIM(LOWER(last_name)),
                NIP = REPLACE((REPLACE(NIP, ' ', '')), '-', ''),
                company_name = TRIM(LOWER(company_name)),
                email = TRIM(email),
                phone_number_1  = TRIM(phone_number_1),
                phone_number_2  = TRIM(phone_number_2),
                login = TRIM(login),
                adress = LOWER(adress)
            """)


c.execute("""UPDATE customers
            SET first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END,
                last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END,
                NIP = CASE NIP WHEN '' THEN NULL ELSE NIP END,
                company_name = CASE company_name WHEN '' THEN NULL ELSE company_name END,
                email = CASE email WHEN '' THEN NULL ELSE email END,
                phone_number_1  = CASE phone_number_1 WHEN '' THEN NULL ELSE phone_number_1 END,
                phone_number_2  = CASE phone_number_2 WHEN '' THEN NULL ELSE phone_number_2 END,
                login = CASE login WHEN '' THEN NULL ELSE login END,
                adress = CASE adress WHEN '' THEN NULL ELSE adress END
            """)

c.execute("""CREATE TABLE prepared_customers(
                first_name text,
                last_name text,
                NIP text,
                company_name text,
                email text,
                phone_number_1 text,
                phone_number_2 text,
                login text,
                id integer,
                adress text)
                """)
c.execute("""INSERT INTO prepared_customers 
          (first_name, last_name, NIP,company_name, email, phone_number_1, phone_number_2,
          login, id, adress) 
          SELECT DISTINCT
          first_name, last_name, NIP,company_name, email, phone_number_1, phone_number_2,
          login, id, adress
          FROM customers
          """)
conn.commit()




