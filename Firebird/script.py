import kinterbasdb
from faker import Faker
import time
import sys

connection = kinterbasdb.connect(
    host='Localhost',
    database='path_to_database.fdb',
    user='SYSDBA',
    password='firebirdxcassandra',
    charset='UTF8'
)

cursor = connection.cursor()
fake = Faker()

def CreateTables():
    cursor.execute('''
        CREATE TABLE aluno (
            idaluno INTEGER PRIMARY KEY,
            nome VARCHAR(255),
            sexo CHAR(1),
            endereco VARCHAR(255),
            cidade VARCHAR(255),
            uf CHAR(2)
        )
    ''')

    cursor.execute('''
        CREATE TABLE alunodetalhe (
            idaluno INTEGER PRIMARY KEY,
            datanasc DATE,
            rendafamiliar INTEGER,
            notaenem INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE alunocurso (
            idaluno INTEGER,
            idcurso INTEGER,
            curriculo INTEGER,
            creditosvencidos INTEGER,
            PRIMARY KEY (idaluno, idcurso)
        )
    ''')

    cursor.execute('''
        CREATE TABLE curso (
            idcurso INTEGER PRIMARY KEY,
            nome VARCHAR(255),
            sigla VARCHAR(10),
            titulacao VARCHAR(50),
            campus VARCHAR(255)
        )
    ''')

def Inserts():
    # INSERT TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            INSERT INTO aluno(idaluno, nome, sexo, endereco, cidade, uf) 
            VALUES(?, ?, ?, ?, ?, ?)
        ''', (i, fake.first_name(), fake.random_element(elements=['M', 'F']), fake.address(), fake.city(), fake.state_abbr()))
    end_time = time.time()
    print("Insert time TABLE aluno: " + str(end_time - start_time))
    insert_time = end_time - start_time

    # INSERT TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            INSERT INTO alunodetalhe(idaluno, datanasc, rendafamiliar, notaenem) 
            VALUES(?, ?, ?, ?)
        ''', (i, fake.date_of_birth(minimum_age=18, maximum_age=30), fake.random_int(min=1000, max=5000), fake.random_int(min=0, max=1000)))
    end_time = time.time()
    print("Insert time TABLE alunodetalhe: " + str(end_time - start_time))
    insert_time += end_time - start_time

    # INSERT TABELA alunocurso
    start_time = time.time()

    valoridcurso = fake.random_int(min=1, max=7)

    for i in range(int(sys.argv[1])):
        cursor.execute('''
            INSERT INTO alunocurso(idaluno, idcurso, curriculo, creditosvencidos) 
            VALUES(?, ?, ?, ?)
        ''', (i, valoridcurso, fake.random_int(min=100001, max=199999), fake.random_int(min=1, max=1000)))
    end_time = time.time()
    print("Insert time TABLE alunocurso: " + str(end_time - start_time))
    insert_time += end_time - start_time

    # INSERT TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            INSERT INTO curso(idcurso, nome, sigla, titulacao, campus) 
            VALUES(?, ?, ?, ?, ?)
        ''', (i, fake.random_element(elements=['Computer Science', 'Business', 'Engineering']), fake.random_element(elements=['CS', 'BUS', 'ENG']), fake.random_element(elements=['B.Sc', 'M.Sc', 'Ph.D']), fake.city()))
    end_time = time.time()
    print("Insert time TABLE curso: " + str(end_time - start_time))
    insert_time += end_time - start_time

    print("Total insert time: " + str(insert_time))

def Updates():
    # UPDATE TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            UPDATE aluno SET endereco = ?, cidade = ? WHERE idaluno = ?
        ''', (fake.address(), fake.city(), i))
    end_time = time.time()
    print("Update time TABLE aluno: " + str(end_time - start_time))

    # UPDATE TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            UPDATE alunodetalhe SET rendafamiliar = ?, notaenem = ? WHERE idaluno = ?
        ''', (fake.random_int(min=1000, max=5000), fake.random_int(min=0, max=1000), i))
    end_time = time.time()
    print("Update time TABLE alunodetalhe: " + str(end_time - start_time))

    # UPDATE TABELA alunocurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            UPDATE alunocurso SET curriculo = ?, creditosvencidos = ? WHERE idaluno = ?
        ''', (fake.random_int(min=100001, max=199999), fake.random_int(min=1, max=1000), i))
    end_time = time.time()
    print("Update time TABLE alunocurso: " + str(end_time - start_time))

    # UPDATE TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            UPDATE curso SET nome = ?, sigla = ?, titulacao = ?, campus = ? WHERE idcurso = ?
        ''', (fake.random_element(elements=['Computer Science', 'Business', 'Engineering']), fake.random_element(elements=['CS', 'BUS', 'ENG']), fake.random_element(elements=['B.Sc', 'M.Sc', 'Ph.D']), fake.city(), i))
    end_time = time.time()
    print("Update time TABLE curso: " + str(end_time - start_time))

def Deletes():
    # DELETE TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            DELETE FROM aluno WHERE idaluno = ?
        ''', (i,))
    end_time = time.time()
    print("Delete time TABLE aluno: " + str(end_time - start_time))

    # DELETE TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            DELETE FROM alunodetalhe WHERE idaluno = ?
        ''', (i,))
    end_time = time.time()
    print("Delete time TABLE alunodetalhe: " + str(end_time - start_time))

    # DELETE TABELA alunocurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            DELETE FROM alunocurso WHERE idaluno = ?
        ''', (i,))
    end_time = time.time()
    print("Delete time TABLE alunocurso: " + str(end_time - start_time))

    # DELETE TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cursor.execute('''
            DELETE FROM curso WHERE idcurso = ?
        ''', (i,))
    end_time = time.time()
    print("Delete time TABLE curso: " + str(end_time - start_time))

CreateTables()
Inserts()
Updates()
Deletes()

connection.commit()
cursor.close()
connection.close()
