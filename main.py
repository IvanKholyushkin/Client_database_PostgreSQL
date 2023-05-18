import psycopg2
from config import host, user, password, port


def create_table(conn, cur):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                PRIMARY KEY (client_id),
                client_id   SERIAL      NOT NULL,
                first_name  VARCHAR(20) NOT NULL,
                last_name   VARCHAR(20) NOT NULL,
                email       VARCHAR(64) UNIQUE NOT NULL
            );
            """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS phone(
                PRIMARY KEY  (phone_id),
                phone_id     SERIAL      NOT NULL,
                phone_number VARCHAR(20) UNIQUE,
                client_id    INT,
                FOREIGN KEY  (client_id) REFERENCES client(client_id)
            );
            """)
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    cur.execute("""
            INSERT INTO client(first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING client_id, first_name, last_name;
            """, (first_name, last_name, email))
    new_client = cur.fetchone()
    print(f"Client {new_client[1]} {new_client[2]} added in the database under id {new_client[0]}")
    if phones is not None:
        add_phone(conn, phones, new_client[0])


def add_phone(conn, phone_number, client_id):
    cur.execute("""
            INSERT INTO phone(phone_number, client_id)
            VALUES (%s, %s) RETURNING phone_number, client_id;
            """, (phone_number, client_id))
    number = cur.fetchone()
    print(f"Phone number {number[0]} for client with id {number[1]} added to the database")


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    if first_name is not None:
        cur.execute("""
                UPDATE client
                   SET first_name = %s
                 WHERE client_id = %s RETURNING first_name;
                """, (first_name, client_id))
        update = cur.fetchone()
        if update is None:
            print(f"Client with id: {client_id} not found")
        else:
            print(f"Client's first name has been changed to {update}")

    if last_name is not None:
        cur.execute("""
                UPDATE client
                   SET last_name = %s
                 WHERE client_id = %s RETURNING last_name;
                """, (last_name, client_id))
        update = cur.fetchone()
        if update is None:
            print(f"Client with id: {client_id} not found")
        else:
            print(f"Client's last name has been changed to {update}")

    if email is not None:
        cur.execute("""
                UPDATE client
                   SET email = %s
                 WHERE client_id = %s RETURNING email;
                """, (email, client_id))
        update = cur.fetchone()
        if update is None:
            print(f"Client with id: {client_id} not found")
        else:
            print(f"Client's email has been changed to {update}")

    if phones is not None:
        cur.execute("""
                UPDATE phone
                   SET phone_number = %s
                 WHERE client_id = %s RETURNING phone_number;
                """, (phones, client_id))
        update = cur.fetchone()
        if update is None:
            print(f"Client with id: {client_id} not found")
        else:
            print(f"Client's phone number has been changed to {update}")


def delete_phone(conn, client_id, phone_number):
    cur.execute("""
            DELETE 
              FROM phone
             WHERE client_id = %s AND phone_number = %s RETURNING phone_number; 
            """, (client_id, phone_number))
    delete = cur.fetchone()
    if delete is None:
        print("Number not found in database")
    else:
        print(f"Client's phone number {delete} successfully removed")


def delete_client(conn, client_id):
    cur.execute("""
            DELETE 
              FROM phone
             WHERE client_id = %s RETURNING client_id;
            """, (client_id,))
    cur.execute("""
            DELETE 
              FROM client
             WHERE client_id = %s RETURNING client_id;
            """, (client_id,))
    delete = cur.fetchone()
    if delete is None:
        print("Client was not found in the database")
    else:
        print(f"Client with id {delete} successfully removed")


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    cur.execute("""
            SELECT first_name, last_name, email, phone_number
              FROM client
              LEFT JOIN phone USING (client_id)
             WHERE first_name = %s OR last_name = %s OR email = %s OR phone_number = %s;
            """, (first_name, last_name, email, phone))
    information = cur.fetchall()
    if not information:
        print("Client with the entered data is not in the database")
    else:
        print(information)


if __name__ == '__main__':
    with psycopg2.connect(
           database="client_database",
            user=user,
            password=password,
            host=host,
            port=port
    ) as connection:
        with connection.cursor() as cur:

            # создаём структуры БД (клиент, телефон)
            create_table(connection, cur)

            # добавляем новых клиентов
            add_client(connection, "Joe", "Fearless", "joe1986@mail.ru", "+79266847233")
            add_client(connection, "Maria", "Unambiguous", "soup88@mail.com")
            add_client(connection, "Ivan", "Nepomniachtchi", "iva@yandex.ru", "+79996415026")
            add_client(connection, "Kurt", "Best", "kurtbestl88@gmail.com", "+79266925128")
            add_client(connection, "Genadiy", "Samoletov", "victor1784@rambler.ru")
            add_client(connection, "Anton", "Klyuchikov", "klyuch_anton666@mail.ru", "+79267259026")
            add_client(connection, "Alexander", "Bezshutnikov", "alex2021@gmail.org", "89656692502")
            add_client(connection, "Alexander", "Ivanov", "alex_ivanov@gmail.org", "89656502139")
            add_client(connection, "Alexander", "Chico", "chico@mail.ru", "+79996664411")

            # добавляем телефон для существующего клиента
            add_phone(connection, "89664432211", 2)
            add_phone(connection, "89664432212", 2)
            add_phone(connection, "89657672213", 7)

            # меняем данные клиента
            change_client(connection, 6, "John")
            change_client(connection, 4, None, "Cobain")
            change_client(connection, 90, None, None, None, "+72633257871")

            # удаляем телефон для существующего клиента
            delete_phone(connection, 2, "89664432211")
            delete_phone(connection, 12, "89664546711")

            # удаляем существующего клиента
            delete_client(connection, 7)
            delete_client(connection, 1)
            delete_client(connection, 14)

            # находим клиента по его данным: имени, фамилии, email или телефону
            find_client(connection, None, "Nepomniachtchi")
            find_client(connection, "Kurt")
            find_client(connection, None, None, "victor1784@rambler.ru")
            find_client(connection, None, None, None, "89656502139")
            find_client(connection, "Alexander")
            find_client(connection, "Stepan")





