import mysql.connector
import MySQLdb
import MySQLdb.cursors
import os

MYSQL_CNF = os.path.abspath('.') + '/mysql.cnf'
mydb = mysql.connector.connect(
    host = "localhost",
    user = "szymon",
    passwd = "Szympek98",
    database = "Allegro"
)
my_cursor = mydb.cursor(buffered=True)

write_con = MySQLdb.connect(db = 'Allegro',
                            charset='utf8',
                            read_default_file=MYSQL_CNF)


my_cursor1 = write_con.cursor()

my_cursor.execute("DROP TABLE IF EXISTS accounts3")

my_cursor.execute("""CREATE TABLE accounts3
                (
                   account_id  INTEGER UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                   parent_id INTEGER UNSIGNED NULL DEFAULT NULL,
                   first_name varchar(255) NULL DEFAULT  NULL,
                   last_name varchar(255) NULL DEFAULT  NULL,
                   NIP varchar(255) NULL DEFAULT  NULL,
                   company_name varchar(255) NULL DEFAULT  NULL,
                   email varchar(255) NULL DEFAULT NULL,
                   email_status INTEGER NULL DEFAULT NULL,
                   phone_1 varchar(255) NULL DEFAULT NULL,
                   phone_2 varchar(255) NULL DEFAULT NULL,
                   login varchar(255) NULL DEFAULT NULL,
                   adress varchar(255) NULL DEFAULT NULL,
                   old_private_id INTEGER UNSIGNED NULL DEFAULT NULL
    
                )""" )

#----------------ADD FOREIGN KEY TO BUILD HIERARCHICAL STRUCTURE----------------
my_cursor.execute("""ALTER TABLE accounts3 ADD FOREIGN KEY (parent_id)
                     REFERENCES accounts3 (account_id)
                  """)

my_cursor.execute("""INSERT INTO accounts3 (parent_id, first_name) VALUES 
                    (NULL, 'ALL_ACCOUNTS'),
                    (1, 'Private_accounts'),
                    (1, 'Company_accounts')
""")

my_cursor.execute("""INSERT INTO accounts3 (NIP, parent_id ,first_name)
                  SELECT NIP, 3, 'companies'
                  FROM uniq_NIPs
                  """)

my_cursor.execute("""INSERT INTO accounts3 (parent_id, first_name, last_name, old_private_id, login) 
                    SELECT 2, first_name, last_name, canon_id, 'private_customer_basic'
                    FROM private_customers
                    WHERE canon_id = id
        """)

my_cursor.execute("DROP TABLE IF EXISTS privates_id")

my_cursor.execute("""CREATE TABLE privates_id
                    AS 
                    (
                    SELECT *
                    FROM accounts3
                    WHERE login = 'private_customer_basic'
                    )""")

my_cursor.execute("DROP TABLE IF EXISTS Companies_id")

my_cursor.execute("""CREATE TABLE Companies_id
                    AS 
                    (
                    SELECT NIP, account_id
                    FROM accounts3
                    WHERE first_name = 'companies'
                    )""")

my_cursor.execute("DROP TABLE IF EXISTS deduped_customers")

my_cursor.execute("""CREATE TABLE deduped_customers
                        AS 
                        (
                        SELECT entity_map.id, entity_map.canon_id, prepared_customers.first_name,
                        prepared_customers.last_name, prepared_customers.NIP,prepared_customers.company_name, prepared_customers.email,
                        prepared_customers.phone_number_1, prepared_customers.phone_number_2, 
                        prepared_customers.login, prepared_customers.adress
                        FROM entity_map
                        JOIN prepared_customers
                        ON entity_map.id = prepared_customers.id
                        WHERE entity_map.id = entity_map.canon_id AND NIP IS NOT NULL
                        ORDER BY entity_map.canon_id
                        )
                    """)


my_cursor.execute("DROP TABLE IF EXISTS private_customers")

my_cursor.execute("""CREATE TABLE private_customers
                        AS 
                        (
                        SELECT entity_map.id, entity_map.canon_id, prepared_customers.first_name,
                        prepared_customers.last_name, prepared_customers.NIP,prepared_customers.company_name, prepared_customers.email,
                        prepared_customers.phone_number_1, prepared_customers.phone_number_2, 
                        prepared_customers.login, prepared_customers.adress
                        FROM entity_map
                        JOIN prepared_customers
                        ON entity_map.id = prepared_customers.id
                        WHERE NIP IS NULL
                        ORDER BY entity_map.canon_id
                        )
                    """)


my_cursor.execute("DROP TABLE IF EXISTS uniq_NIPs")

my_cursor.execute("""CREATE TABLE uniq_NIPs
                    AS
                    (
                    SELECT DISTINCT NIP
                    FROM deduped_customers
                    WHERE NIP IS NOT NULL
                    )""")


my_cursor.execute("DROP PROCEDURE IF EXISTS ROWPERROW;")

my_cursor.execute("""
                    CREATE PROCEDURE ROWPERROW()
                    BEGIN
                    DECLARE n INT DEFAULT 0;
                    DECLARE i INT DEFAULT 0;
                    SELECT COUNT(*) FROM deduped_customers INTO n;
                    SET i=0;
                    WHILE i<n DO 
                        
                      INSERT INTO accounts3 (parent_id, first_name,last_name, NIP, company_name, email, email_status, phone_1,phone_2, login , adress) 
                      SELECT return_id_NIP(NIP), first_name, last_name, NIP, company_name, email, 1, phone_number_1, phone_number_2, login, adress FROM deduped_customers LIMIT i,1;
                      SET i = i + 1;
                    END WHILE;
                    End;
                    ;;
                
""")
my_cursor.execute("DROP PROCEDURE IF EXISTS ROWPERROW1;")
my_cursor.execute("""
                    CREATE PROCEDURE ROWPERROW1()
                    BEGIN
                    DECLARE n INT DEFAULT 0;
                    DECLARE i INT DEFAULT 0;
                    SELECT COUNT(*) FROM private_customers INTO n;
                    SET i=0;
                    WHILE i<n DO 

                      INSERT INTO accounts3 (parent_id, first_name,last_name, NIP, company_name, email, email_status, phone_1,phone_2, login , adress) 
                      SELECT return_id_oldpriv(canon_id), first_name, last_name, NIP, company_name, email, 1, phone_number_1, phone_number_2, login, adress FROM private_customers LIMIT i,1;
                      SET i = i + 1;
                    END WHILE;
                    End;
                    ;;

""")


my_cursor.execute("CALL ROWPERROW()")
my_cursor.execute("CALL ROWPERROW1()")

my_cursor.execute("DROP FUNCTION IF EXISTS return_id_NIP")

my_cursor.execute("""CREATE FUNCTION return_id_NIP (name1 varchar(255)) RETURNS INTEGER
                    DETERMINISTIC
                    BEGIN
                        DECLARE ID_result INT;
                        SELECT account_id INTO ID_result FROM Companies_id WHERE NIP = name1;
                        RETURN ID_result;
                        END;
#                         """)




my_cursor1.execute("DROP FUNCTION IF EXISTS return_id_oldpriv;")
my_cursor1.execute("""CREATE FUNCTION return_id_oldpriv (name1 varchar(255)) RETURNS INTEGER
                    DETERMINISTIC
                    BEGIN
                        DECLARE ID_result INT;
                        SELECT account_id INTO ID_result FROM Companies_id WHERE old_private_id = name1;
                        RETURN ID_result;
                        END;
#                         """)
