from cassandra.cluster import Cluster
from faker import Faker
import time
import sys


cluster = Cluster(['localhost'])  
session = cluster.connect()

fake = Faker()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS trabalhofinal
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
""")
session.set_keyspace('trabalhofinal')


def CreateTables():

    session.execute("""
        CREATE TABLE IF NOT EXISTS aluno (
            idaluno INT PRIMARY KEY,
            nome TEXT,
            sexo TEXT,
            endereco TEXT,
            cidade TEXT,
            uf TEXT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS alunodetalhe (
            idaluno INT PRIMARY KEY,
            datanasc DATE,
            rendafamiliar INT,
            notaenem INT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS alunocurso (
            idaluno INT,
            idcurso INT,
            curriculo INT,
            creditosvencidos INT,
            PRIMARY KEY (idaluno, idcurso)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS curso (
            idcurso INT PRIMARY KEY,
            nome TEXT,
            sigla TEXT,
            titulacao TEXT,
            campus TEXT
        )
    """)


def Inserts():
    #INSERT TABELA aluno 
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"""
            INSERT INTO aluno(idaluno, nome, sexo, endereco, cidade, uf) 
            VALUES({i}, '{fake.first_name()}', '{fake.random_element(elements=['M', 'F'])}', '{fake.address()}', '{fake.city()}', '{fake.state_abbr()}')
        """)
    end_time = time.time()
    print("Insert time TABLE aluno: " + str(start_time-end_time))
    insert_time = start_time-end_time


    #INSERT TABELA alunodetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"""
            INSERT INTO alunodetalhe(idaluno, datanasc, rendafamiliar, notaenem) 
            VALUES({i}, '{fake.date_of_birth(minimum_age=18, maximum_age=30)}', {fake.random_int(min=1000, max=5000)}, {fake.random_int(min=0, max=1000)})
        """)
    end_time = time.time()
    print("Insert time TABLE alunodetalhe: " + str(start_time-end_time))
    insert_time += start_time-end_time

    #INSERT TABELA alunocurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"""
            INSERT INTO alunocurso(idaluno, idcurso, curriculo, creditosvencidos) 
            VALUES({i}, {fake.random_int(min=1, max=7)}, {fake.random_int(min=100001, max=199999)}, {fake.random_int(min=1, max=1000)})
        """)
    end_time = time.time()
    print("Insert time TABLE alunocurso: " + str(start_time-end_time))
    insert_time += start_time-end_time

    #INSERT TABELA curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"""
            INSERT INTO curso(idcurso, nome, sigla, titulacao, campus) 
            VALUES({i}, '{fake.random_element(elements=['ciencia da computacao','Engenharia Agricola', 'Engenharia civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica'])}', '{fake.random_element(elements = ['CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM'])}', '{fake.random_element(elements = ['Bacharelado', 'Tecnologo', 'Licenciatura'])}', '{fake.random_element(elements = ['Alegrete', 'Bage', 'caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja'])}')
        """)
    end_time = time.time()
    print("Insert time TABLE curso: " + str(start_time-end_time))
    insert_time += start_time-end_time

    insert_time = insert_time/4
    print("Media dos Inserts: " + str(insert_time))

def Updates():
    #UPDATE TABELA aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"""
            UPDATE aluno SET 
            nome = '{fake.first_name()}', 
            sexo = '{fake.random_element(elements=['M', 'F'])}', 
            endereco = '{fake.address()}', cidade = '{fake.city()}', 
            uf = '{fake.state_abbr()}' 
            WHERE idaluno = '{fake.random_int(min=1, max=int(sys.argv[1]))}'
        """)
    end_time = time.time()
    print("Update time TABLE Aluno: " + str(start_time-end_time))
    update_time = start_time-end_time

    #UPDATE TABELA AlunoDetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"UPDATE TrabalhoFinal.AlunoDetalhe SET  DataNasc = '{fake.date_of_birth(minimum_age=18, maximum_age=30)}', RendaFamiliar = {fake.random_int(min=1000, max=5000)}, NotaEnem = {fake.random_int(min=0, max=1000)} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[1]))}")
    end_time = time.time()
    print("Update time TABLE AlunoDetalhe: " + str(start_time-end_time)) 
    update_time += start_time-end_time

    #UPDATE TABELA AlunoCurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"UPDATE TrabalhoFinal.AlunoCurso SET Curriculo = {fake.random_int(min=100001, max=199999)}, CreditosVencidos = {fake.random_int(min=1, max=1000)} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[1]))}")
    end_time = time.time()
    update_time += start_time-end_time
    print("Update time TABLE AlunoCurso: " + str(start_time-end_time))

    #UPDATE TABELA Curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"UPDATE TrabalhoFinal.Curso SET Nome = '{fake.random_element(elements=['Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica'])}', Sigla = '{fake.random_element(elements =['CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM'])}', Titulacao = '{fake.random_element(elements = ['Bacharelado', 'Tecnologo', 'Licenciatura'])}', Campus = '{fake.random_element(elements = ['Alegrete', 'Bage', 'Caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja'])}' WHERE IdAluno = '{fake.random_int(min=1, max=int(sys.argv[1]))}'")
    end_time = time.time()
    print("Update time TABLE Curso: " + str(start_time-end_time))
    update_time += start_time-end_time

    update_time = update_time/4
    print("Media dos Updates: " + str(start_time-end_time))


def main():
    CreateTables()
    Inserts()
    Updates()
    cluster.shutdown()

if __name__ == "__main__":
    main()




