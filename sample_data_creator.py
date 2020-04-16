import random
import MySQLdb
import os


conn = MySQLdb.connect(read_default_file = os.path.abspath('.') + '/mysql.cnf',
                       local_infile = 1,
                       sql_mode="ALLOW_INVALID_DATES",
                        db = "Allegro"
                       )


c = conn.cursor()

c.execute("DROP TABLE IF EXISTS customers")


firs_names_list = ["Szymon", "Mateusz", "Jakub", "Gracjan", "Rafał", "Krzysztof", "Jacek", "Barbara",
              "Weronika", "Anastazja", "Jakoda", "Sławomir", "Janusz", "Jarosław",
             "Antoni", "Jan", "Franciszek", "Mikołaj", "Marcel", "Adam"]
last_names_list = ["Kowalczyk", "Ignatiew", "Król" , "Cieślak", "Pyza", "Grzechotka", "Grzebya", "Baran"
                   "Koza", "Miecz", "Żaba", "Kaczyński", "Duda", "Zelner", "Szlachcic"]

NIP_list =["76-661-08783","9187743554","2559406023","1524969650","9731131402","2813385306","2759460352","3797324864",
           "2016552186","8518010828","3963-185377","3835-5752 91","896869-2414","2774332374","2942498550","4434174026",
           "2883925162","8556856510","3710726245","3623198205","6569922484","5848105387","1084606994",None, None, None]

email_list = ["auctor.velit@Nullamsuscipitest.org","Suspendisse@metus.com",
"nunc.nulla@arcuAliquam.ca",
"lectus.justo@eu.net",
"pellentesque.Sed.dictum@morbitristique.edu",
"Donec@vulputateposuerevulputate.org"
"non.dapibus.rutrum@mus.co.uk",
"Cras.eget.nisi@malesuadaid.edu",
"Vivamus.sit.amet@nunc.net",
"Donec@semperpretiumneque.ca",
"a@hymenaeosMauris.co.uk",
"eu.dolor@inmagna.edu",
"dignissim.Maecenas@aliquet.edu",
"sem.Pellentesque.ut@necorci.net",
"metus.facilisis.lorem@nonummy.co.uk"]

#my phone numbers generator
phone_list_1 = []
phone_list_2 = []
for x in range (50):
    t = str(random.randrange(100000000, 999999999))
    z = str(random.randrange(100000000, 999999999))
    phone_list_1.append(t)
    phone_list_2.append(z)

login_list = ["krzysiu1123", "Zoska323", "łysy34", "gruby525", "honorat555", "mały555", "eka386", "zbynio",
              "jolo", "wał", 'przemo534', "hubi", "bierna", "zubi434"]

adres_list = ["orzeszkowa", "zieloma 43", "łysa 32", "tarnoole 54", "zielonka", "domekHałabały", "zamkowa",
              "pomaranczowa", "niebieska", "chaszczowo" ]

company_list = ["POLSOFT", "ENEA", "MBANK", "Czesławski", "Google", "FACEBOOK", "sdf"]

c.execute("""CREATE TABLE customers(
                first_name text,
                last_name text,
                NIP text,
                company_name text,
                email text,
                phone_number_1 text,
                phone_number_2 text,
                login text,
                id integer,
                adress text) """)




#Function to put values into data set


def insert_emp(first_name, last_name, NIP,company_name, email, phone_number_1, phone_number_2, login, id, adress):

    dane = (first_name, last_name, NIP,company_name, email, phone_number_1, phone_number_2, login, id, adress)

    formula = "INSERT INTO customers (first_name, last_name, NIP,company_name, email, phone_number_1, phone_number_2, login, id, adress) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"
    c.execute(formula, dane)
    conn.commit()


for i in range (10000):
    insert_emp(random.choice(firs_names_list), random.choice(last_names_list), random.choice(NIP_list), random.choice(company_list), random.choice(email_list),
               random.choice(phone_list_1), random.choice(phone_list_2), random.choice(login_list), i , random.choice(adres_list))


c.execute(""" SELECT * FROM customers
            """)
print(c.fetchall())