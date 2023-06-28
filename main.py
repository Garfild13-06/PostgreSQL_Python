# -*- coding: utf-8 -*-

import psycopg2


class Postgresql:
    def __init__(self):
        try:
            dbname = "netology_db"
            username = "postgres"
            password = ""
            self.con = psycopg2.connect(dbname=dbname, user=username, password=password)
            self.con.set_client_encoding("utf-8")
        except Exception as e:
            print(e)

    def get_client_id(self, last_name, first_name):
        """Функция для получения id клиента по фамилии и имени"""
        try:
            with self.con.cursor() as cur:
                cur.execute("SELECT id FROM public.clients  WHERE last_name=%s and first_name=%s;",
                            (last_name, first_name))
                res = cur.fetchone()

                if len(res) > 0:
                    if res is not None:
                        client_id = res[0]
                        return client_id
                    else:
                        print("row is None")
                else:
                    print("Не указаны данные для поиска клиента")
        except Exception as e:
            print(e)

    def create_db(self):
        """Функция, создающая структуру БД (таблицы)."""
        try:
            with self.con.cursor() as cur:
                cur.execute("""CREATE TABLE public.clients (
                first_name varchar NOT NULL,
                last_name varchar NOT NULL,
                email varchar NULL,
                id serial4 NOT NULL,
                CONSTRAINT clients_pk PRIMARY KEY (id)
                );""")

                cur.execute("""CREATE TABLE public.clients_phones (
                    phone varchar(11) NOT NULL,
                    client_id int4 NOT NULL,
                    CONSTRAINT clients_phones_un UNIQUE (phone),
                    CONSTRAINT clients_phones_fk FOREIGN KEY (client_id) REFERENCES public.clients(id)
                );""")
                self.con.commit()
        except Exception as e:
            print(e)

    def new_client(self, first_name, last_name, email, phone=""):
        """Функция, позволяющая добавить нового клиента"""
        try:
            with self.con.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.clients (first_name, last_name, email) VALUES(%s, %s, %s) RETURNING id;",
                    (first_name, last_name, email))
                if phone == "":
                    self.con.commit()
                else:
                    client_id = cur.fetchone()[0]
                    cur.execute("INSERT INTO public.clients_phones (phone, client_id) VALUES(%s, %s);",
                                (phone, client_id))
                    self.con.commit()
        except Exception as e:
            print(e)
        finally:
            self.close()

    def add_phone(self, phone, last_name, first_name):
        """Функция, позволяющая добавить телефон для существующего клиента"""
        try:
            with self.con.cursor() as cur:
                client_id = self.get_client_id(last_name, first_name)
                cur.execute("""                        
                            INSERT INTO public.clients_phones (phone, client_id)
                            VALUES(%s, %s);
                                """,
                            (phone, client_id))
                self.con.commit()

        except Exception as e:
            print(e)
        finally:
            self.close()

    def change_data(self, cur_last_name, cur_first_name, new_last_name="", new_first_name="", new_email=""):
        """Функция, позволяющая изменить данные о клиенте"""
        try:
            with self.con.cursor() as cur:
                client_id = self.get_client_id(cur_last_name, cur_first_name)
                if new_last_name != "" and new_first_name != "" and new_email != "":
                    cur.execute(
                        "UPDATE public.clients SET first_name=%s, last_name=%s, email=%s WHERE id=%s;",
                        (new_first_name, new_last_name, new_email, client_id))
                    self.con.commit()
                elif new_last_name != "" and new_first_name == "" and new_email == "":
                    cur.execute(
                        "UPDATE public.clients SET last_name=%s WHERE id=%s;",
                        (new_last_name, client_id))
                    self.con.commit()
                elif new_last_name == "" and new_first_name != "" and new_email == "":
                    cur.execute(
                        "UPDATE public.clients SET first_name=%s WHERE id=%s;",
                        (new_first_name, client_id))
                    self.con.commit()
                elif new_last_name == "" and new_first_name == "" and new_email != "":
                    cur.execute(
                        "UPDATE public.clients SET email=%s WHERE id=%s;",
                        (new_email, client_id))
                    self.con.commit()
                elif new_last_name != "" and new_first_name != "" and new_email == "":
                    cur.execute(
                        "UPDATE public.clients SET last_name=%s, first_name=%s WHERE id=%s;",
                        (new_last_name, new_first_name, client_id))
                    self.con.commit()
                elif new_last_name != "" and new_first_name == "" and new_email != "":
                    cur.execute(
                        "UPDATE public.clients SET last_name=%s, email=%s WHERE id=%s;",
                        (new_last_name, new_email, client_id))
                    self.con.commit()
                elif new_last_name == "" and new_first_name != "" and new_email != "":
                    cur.execute(
                        "UPDATE public.clients SET first_name=%s, email=%s WHERE id=%s;",
                        (new_first_name, new_email, client_id))
                    self.con.commit()
                elif cur_last_name == new_last_name or cur_first_name == new_first_name:
                    print("Новые данные совпадают с текущими")
                else:
                    print("Не указаны новые данные")
        except Exception as e:
            print(e)
        finally:
            self.close()

    def delete_phone(self, last_name, first_name):
        """Функция, позволяющая удалить телефон для существующего клиента."""
        try:
            with self.con.cursor() as cur:
                client_id = self.get_client_id(last_name, first_name)
                cur.execute(
                    "DELETE FROM public.clients_phones WHERE client_id =%s;",
                    (client_id,))
                self.con.commit()
        except Exception as e:
            print(e)
        finally:
            self.close()

    def delete_client(self, last_name, first_name):
        """Функция, позволяющая удалить существующего клиента"""
        try:
            with self.con.cursor() as cur:
                client_id = self.get_client_id(last_name, first_name)
                cur.execute(
                    "DELETE FROM public.clients_phones WHERE client_id =%s;",
                    (client_id,))
                cur.execute(
                    "DELETE FROM public.clients WHERE id =%s;",
                    (client_id,))
                self.con.commit()
                pass
        except Exception as e:
            print(e)
        finally:
            self.close()

    def find_client(self, first_name="", last_name="", email="", phone=""):
        """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону"""
        try:
            with self.con.cursor() as cur:
                if first_name != "":
                    cur.execute("SELECT c.first_name, c.last_name, c.email FROM public.clients c WHERE first_name=%s;",
                                (first_name,))
                    res = cur.fetchone()
                elif last_name != "":
                    cur.execute("SELECT c.first_name, c.last_name, c.email FROM public.clients c WHERE last_name=%s;",
                                (last_name,))
                    res = cur.fetchone()
                elif email != "":
                    cur.execute("SELECT c.first_name, c.last_name, c.email FROM public.clients c WHERE email=%s;",
                                (email,))
                    res = cur.fetchone()
                elif phone != "":
                    cur.execute("""
                        SELECT c.first_name, c.last_name, c.email
                        FROM clients_phones cp
                        LEFT JOIN clients c ON cp.client_id = c.id
                        WHERE phone = '79636079344';""")
                    res = cur.fetchone()
                print(res)
        except Exception as e:
            print(e)
        finally:
            self.close()

    def close(self):
        self.con.close()


# pgsql = Postgresql()

# pgsql.create_db()
# pgsql.new_client(first_name="Алексей", last_name="Марулиди", email="garfild13-06@yandex.com")
# pgsql.add_phone(phone="79636079344", first_name="Алексей", last_name="Марулиди")
# pgsql.delete_phone(first_name="Алексей", last_name="Марулиди")
# pgsql.change_data(
#     cur_last_name="Марулиди",
#     cur_first_name="Алексей",
#     new_last_name="Пётр",
#     new_first_name="Иванов",
#     new_email="ivanov_p@yandex.com")
# pgsql.delete_client(last_name="Марулиди",first_name="Алексей")
# pgsql.find_client(last_name="Марулиди")

# pgsql.close()
