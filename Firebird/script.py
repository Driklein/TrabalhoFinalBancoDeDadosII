import os
import firebirdsql
from faker import Faker
import time
import sys

fake = Faker()
    
con = firebirdsql.connect(
    host='localhost',
    database='/usr/local/lib/python3.10/dist-packages/firebirdsql/database.fdb',
    user='sysdba',
    password='123'
)

cur = con.cursor()

def CreateTables():
    cur.execute("""
        CREATE TABLE aluno (
            idaluno INTEGER PRIMARY KEY,
            nome VARCHAR(255),
            sexo VARCHAR(255),
            endereco VARCHAR(255),
            cidade VARCHAR(255),
            uf VARCHAR(255)
        );
    """)

    cur.execute("""
        CREATE TABLE alunodetalhe (
            idaluno INTEGER PRIMARY KEY,
            datanasc DATE,
            rendafamiliar INTEGER,
            notaenem INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE alunocurso (
            idaluno INTEGER,
            idcurso INTEGER,
            curriculo INTEGER,
            creditosvencidos INTEGER,
            PRIMARY KEY (idaluno, idcurso)
        );
    """)

    cur.execute("""
        CREATE TABLE curso (
            idcurso INTEGER PRIMARY KEY,
            nome VARCHAR(255),
            sigla VARCHAR(255),
            titulacao VARCHAR(255),
            campus VARCHAR(255)
        );
    """)

def Inserts():
    # INSERT TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            INSERT INTO aluno(idaluno, nome, sexo, endereco, cidade, uf)
            VALUES({i}, '{fake.first_name()}', '{fake.random_element(elements=['M', 'F'])}', '{fake.address()}', '{fake.city()}', '{fake.state_abbr()}')
        """)
    end_time = time.time()
    print("Insert time TABLE aluno: " + str(start_time - end_time))
    insert_time = start_time - end_time

    # INSERT TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            INSERT INTO alunodetalhe(idaluno, datanasc, rendafamiliar, notaenem)
            VALUES({i}, '{fake.date_of_birth(minimum_age=18, maximum_age=30)}', {fake.random_int(min=1000, max=5000)}, {fake.random_int(min=0, max=1000)})
        """)
    end_time = time.time()
    print("Insert time TABLE alunodetalhe: " + str(start_time - end_time))
    insert_time += start_time - end_time

    # INSERT TABELA alunocurso
    start_time = time.time()
    valoridcurso = fake.random_int(min=1, max=7)
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            INSERT INTO alunocurso(idaluno, idcurso, curriculo, creditosvencidos)
            VALUES({i}, {valoridcurso}, {fake.random_int(min=100001, max=199999)}, {fake.random_int(min=1, max=1000)})
        """)
    end_time = time.time()
    print("Insert time TABLE alunocurso: " + str(start_time - end_time))
    insert_time += start_time - end_time

    # INSERT TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            INSERT INTO curso(idcurso, nome, sigla, titulacao, campus)
            VALUES({valoridcurso}, '{fake.random_element(elements=['Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica'])}', '{fake.random_element(elements = ['CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM'])}', '{fake.random_element(elements = ['Bacharelado', 'Tecnologo', 'Licenciatura'])}', '{fake.random_element(elements = ['Alegrete', 'Bage', 'caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja'])}')
        """)
    end_time = time.time()
    print("Insert time TABLE curso: " + str(start_time - end_time))
    insert_time += start_time - end_time

    insert_time = insert_time / 4
    print("Media dos Inserts: " + str(insert_time))

def Updates():
    # UPDATE TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            UPDATE aluno SET 
            nome = '{fake.first_name()}', 
            sexo = '{fake.random_element(elements=['M', 'F'])}', 
            endereco = '{fake.address()}', cidade = '{fake.city()}', 
            uf = '{fake.state_abbr()}' 
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))}
        """)
    end_time = time.time()
    print("Update time TABLE aluno: " + str(start_time - end_time))
    update_time = start_time - end_time

    # UPDATE TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            UPDATE alunodetalhe SET  
            datanasc = '{fake.date_of_birth(minimum_age=18, maximum_age=30)}', 
            rendafamiliar = {fake.random_int(min=1000, max=5000)}, 
            notaenem = {fake.random_int(min=0, max=1000)} 
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))}
        """)
    end_time = time.time()
    print("Update time TABLE alunodetalhe: " + str(start_time - end_time))
    update_time += start_time - end_time

    # UPDATE TABELA alunocurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            UPDATE alunocurso SET
            curriculo = {fake.random_int(min=100001, max=199999)}, 
            creditosvencidos = {fake.random_int(min=1, max=1000)} 
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))} AND idcurso = {fake.random_int(min=1, max=7)}
        """)
    end_time = time.time()
    update_time += start_time - end_time
    print("Update time TABLE alunocurso: " + str(start_time - end_time))

    # UPDATE TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            UPDATE curso SET 
            nome = '{fake.random_element(elements=['Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica'])}', 
            sigla = '{fake.random_element(elements =['CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM'])}', 
            titulacao = '{fake.random_element(elements = ['Bacharelado', 'Tecnologo', 'Licenciatura'])}', 
            campus = '{fake.random_element(elements = ['Alegrete', 'Bage', 'Caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja'])}' 
            WHERE idcurso = {fake.random_int(min=1, max=7)}
        """)
    end_time = time.time()
    print("Update time TABLE curso: " + str(start_time - end_time))
    update_time += start_time - end_time

    update_time = update_time / 4
    print("Media dos Updates: " + str(update_time))

def Deletes():
    # DELETE TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            DELETE FROM aluno  
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))}
        """)
    end_time = time.time()
    print("Delete time TABLE aluno: " + str(start_time - end_time))
    delete_time = start_time - end_time

    # DELETE TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            DELETE FROM alunodetalhe
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))}
        """)
    end_time = time.time()
    print("Delete time TABLE alunodetalhe: " + str(start_time - end_time))
    delete_time = start_time - end_time

    # DELETE TABELA alunocurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            DELETE FROM alunocurso
            WHERE idaluno = {fake.random_int(min=1, max=int(sys.argv[1]))}
        """)
    end_time = time.time()
    print("Delete time TABLE alunocurso: " + str(start_time - end_time))
    delete_time = start_time - end_time

    # DELETE TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        cur.execute(f"""
            DELETE FROM curso
            WHERE idcurso = {fake.random_int(min=1, max=7)}
        """)
    end_time = time.time()
    print("Delete time TABLE curso: " + str(start_time - end_time))
    delete_time = start_time - end_time

    delete_time = delete_time / 4
    print("Media dos Deletes: " + str(delete_time))

def DropTables():
    cur.execute("DROP TABLE aluno;")
    cur.execute("DROP TABLE alunocurso;")
    cur.execute("DROP TABLE alunodetalhe;")
    cur.execute("DROP TABLE curso;")
    


def main():
    CreateTables()
    Inserts()
    Updates()
    Deletes()
    DropTables()

    con.commit()

    con.close()


if __name__ == "__main__":
    main()
