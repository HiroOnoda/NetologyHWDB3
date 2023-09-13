import psycopg2

#Пишу код исходя из предположения что БД под названием clients_db уже создана

#удаляет всю таблицу
def dropdb(conn):
    with conn.cursor() as cur:
        cur.execute("drop table phones")
        cur.execute("drop table clients")
    conn.commit()

# Создание структуры БД
def createdb(conn):
    with conn.cursor() as cur:
        cur.execute('''
        create table if not exists Clients
        (
            id serial primary key,
            first_name varchar(40) not null,
            last_name varchar(40) not null,
            email varchar(40) not null
        );''')
        cur.execute('''
        create table if not exists Phones
        (
            id serial primary key,
            phone varchar(20) not null,
            client integer references Clients(id) on delete cascade
        );''')
        conn.commit()

#Очистить базу данных от значений
def trunc_all(conn):
    with conn.cursor() as cur:
        cur.execute('''
            truncate Clients restart identity cascade;
            truncate Phones restart identity cascade; 
        ''')


# Добавление нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("insert into Clients(first_name,last_name,email) values(%s, %s, %s) returning id;",(first_name,last_name,email))
        #conn.commit()
        print(cur.fetchone())

#Добавление телефона к существующему клиенту
def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("insert into Phones(phone, client) values(%s, %s) returning id;",(phone,client_id))
        print(cur.fetchone())
    pass

#Изменение данных о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if (first_name != None):
            cur.execute("update clients set first_name = (%s) where id = (%s) returning id;",(first_name,client_id))
        if (last_name != None):
            cur.execute("update clients set last_name = (%s) where id = (%s) returning id;",(last_name,client_id))
        if (email != None):
            cur.execute("update clients set email = (%s) where id = (%s) returning id;", (email, client_id))
        if (phones != None):
            cur.execute("update clients set phones = (%s) where id = (%s) returning id;", (phones, client_id))
        print(cur.fetchone())
    pass

#удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("delete from phones where (client = (%s)) and (phone=(%s)) returning id;",(client_id, phone))
        print(cur.fetchone())
    pass

#удалить все телефоны этого клиента
#функция применяется в процессе удаления клиента для каскадного удаления внешних ключей
def delete_clients_phones(conn,client_id):
    with conn.cursor() as cur:
        cur.execute("delete from phones where client = (%s) returning id;",(client_id))
    conn.commit()
    pass

#удаление клиента
def delete_client(conn,client_id):
    delete_clients_phones(conn,client_id)
    #строго говоря, делать все удаления в одном курсоре было бы эффективнее
    #но вынос в отдельную функцию делает код более читаемым
    with conn.cursor() as cur:
        cur.execute("delete from clients where id = (%s) returning id",(client_id))
        print(cur.fetchone())
    pass

#удаление клиента No2 через cascade
def delete_client2(conn,client_id):
    with conn.cursor() as cur:
        cur.execute("delete from clients where id = (%s) returning id",(client_id))
        print(cur.fetchone())
    pass


#поиск клиента по по его данным: имени, фамилии, email или телефону
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    #Делаю через elif чтобы предотвратить запрос данных по данным от разных клиентов
    #В случае если будет введена информация от разных клиентов(Например, имя и фамилия разных людей),
    # то будет выведен клиент по первым данным в иерархии
    with conn.cursor() as cur:
        if (first_name != None):
            cur.execute("select * from clients where first_name = (%s);", (first_name))
            print(cur.fetchall())
        elif (last_name != None):
            cur.execute("select * from clients where last_name = (%s);", (last_name))
            print(cur.fetchall())
        elif (email != None):
            cur.execute("select * from clients where email = (%s);", (email))
            print(cur.fetchall())
        elif (phone != None):
            #cur.execute("select * from clients c join phones p on p.client=c.id where phone={0} group by c.id;".format(str(phone)))\
            #проблема была в кавычках, почему ее не было в прошлых вызовах?
            cur.execute("select * from clients where id = (select client from phones where phone = '%s');"%(phone))
            print(cur.fetchall())
        pass
    pass

#Отсюда вызываются все функции
with psycopg2.connect(database="clients_db",user='postgres',password='postgres') as conn:
    #dropdb(conn)
    createdb(conn)
    trunc_all(conn)
    add_client(conn, 'A','a','1')
    add_client(conn, 'B','b','2')
    add_phone(conn, '1','123456')
    add_phone(conn, '1', 'qwerty')
    delete_phone(conn,'1','123456')
    delete_client(conn,'2')
    delete_client2(conn,'1')
    #Поиск клиентов
    add_client(conn, 'A', 'a', '1')#id=3
    add_client(conn, 'B', 'b', '2')#id=4
    add_client(conn, 'C', 'c', '3')#id=5
    add_client(conn, 'D', 'd', '4')#id=6
    find_client(conn,'A')
    find_client(conn,None,'b')
    find_client(conn,None,None,'3')
    find_client(conn,'A','b','3')
    add_phone(conn, '3','qwe')
    add_phone(conn, '4', 'rty')
    find_client(conn, 'A', 'b', '3','4')# Выводит клиента с id=3
    find_client(conn,None,None,None,'rty')
    # change_client(conn, '2','C','c','3')
    pass