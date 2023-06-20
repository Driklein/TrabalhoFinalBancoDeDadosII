
import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from faker import Faker
import time

cluster = Cluster(['localhost'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS TrabalhoFinal WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
    
session.execute("USE TrabalhoFinal")
session.execute("CREATE TABLE IF NOT EXISTS Aluno(PRIMARY KEY(IdAluno), Nome TEXT, Sexo TEXT, Endereco TEXT, Cidade TEXT, UF TEXT")
session.execute("CREATE TABLE IF NOT EXISTS AlunoDetalhe(DataNasc INT, RendaFamiliar INT, NotaEnem INT, IdAluno INT PRIMARY KEY)")
session.execute("CREATE TABLE IF NOT EXISTS AlunoCurso(IdAluno INT, IdCurso INT, Curriculo INT, CreditosVencidos INT,PRIMARY KEY (IdAluno, IdCurso))")
session.execute("CREATE TABLE IF NOT EXISTS Curso(IdCurso INT PRIMARY KEY, Nome TEXT, Sigla TEXT, Titulacao TEXT, Campus TEXT)")

#INSERT -----------------------------------------------------------
def Insert():
    fake = Faker()
    
    #INSERT TABELA Aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"INSERT INTO TrabalhoFinal.Aluno(IdAluno, Nome, Sexo, Endereco, Cidade, UF) VALUES({i}, '{fake.first_name()}', '{fake.random_element(elements=['M', 'F'])}', '{fake.address()}', '{fake.city()}', '{fake.state_abbr()}')")
    end_time = time.time()
    print("Insert time TABLE Aluno: " + str(start_time-end_time))
    insert_time = start_time-end_time

    #INSERT TABELA AlunoDetalhe
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"INSERT INTO TrabalhoFinal.AlunoDetalhe(IdAluno, DataNasc, RendaFamiliar, NotaEnem) VALUES({i}, '{fake.date_of_birth(minimum_age=18, maximum_age=30)}', {fake.random_int(min=1000, max=5000)}, {fake.random_int(min=0, max=1000)})")
    end_time = time.time()
    print("Insert time TABLE AlunoDetalhe: " + str(start_time-end_time))
    insert_time += start_time-end_time
    
    #INSERT TABELA AlunoCurso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"INSERT INTO TrabalhoFinal.AlunoCurso(IdAluno, IdCurso, Curriculo, CreditosVencidos) VALUES({i}, {fake.random_int(min=1, max=7)}, {fake.random_int(min=100001, max=199999)}, {fake.random_int(min=1, max=1000)})")
    end_time = time.time()
    print("Insert time TABLE AlunoCurso: " + str(start_time-end_time))
    insert_time += start_time-end_time
    
    #INSERT TABELA Curso
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"INSERT INTO TrabalhoFinal.Curso(IdCurso, Nome, Sigla, Titulacao, Campus) VALUES({i}, '{fake.random_element(elements=['Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica'])}', '{fake.random_element(elements = ['CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM'])}', '{fake.random_element(elements = ['Bacharelado', 'Tecnologo', 'Licenciatura'])}', '{fake.random_element(elements = ['Alegrete', 'Bage', 'Caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja'])}')")
    end_time = time.time()
    print("Insert time TABLE Curso: " + str(start_time-end_time))
    insert_time += start_time-end_time

    insert_time = insert_time/4
    print("Media dos Inserts: " + str(insert_time))

#UPDATE -----------------------------------------------------------
def Update():

    #UPDATE TABELA Aluno
    start_time = time.time()
    for i in range(int(sys.argv[1])):
        session.execute(f"UPDATE TrabalhoFinal.Aluno SET Nome = '{fake.first_name()}', Sexo = '{fake.random_element(elements=['M', 'F'])}', Endereco = '{fake.address()}', Cidade = '{fake.city()}', UF = '{fake.state_abbr()}' WHERE IdAluno = '{fake.random_int(min=1, max=int(sys.argv[1]))}'")
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

    cluster.shutdown()

def main():
    Insert()
    Update()

if __name__ == "__main__":
    main()
