
import sys
from cassandra.cluster import cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import faker
import time

class cass(object):

    def __init__(self):
        self.cluster = Cluster(["Test Cluster"])
        self.session = self.cluster.connect("Keyspace")
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\"class\": \"NetworkTopologyStrategy\", \"datacenter1\" : \"1\"}")
        self.session.execute("USE Keyspace")
        self.session.execute("CREATE TABLE IF NOT EXISTS Aluno(IdAluno int PRIMARY KEY, Nome text, Sexo text, Endereco text, Cidade text, UF text)")
        self.session.execute("CREATE TABLE IF NOT EXISTS AlunoDetalhe(IdAluno int FOREIGN KEY, DataNasc date, RendaFamiliar int, NotaEnem int)")
        self.session.execute("CREATE TABLE IF NOT EXISTS AlunoCurso(IdAluno int FOREIGN KEY, IdCurso int FOREIGN KEY, Curriculo int, CreditosVencidos int)")
        self.session.execute("CREATE TABLE IF NOT EXISTS Curso(IdCurso int PRIMARY KEY, Nome text, Sigla text, Titulacao text, Campus text)")

    #INSERT -----------------------------------------------------------
    def Insert(self):
        
        fake = Faker()
        
        #INSERT TABELA Aluno
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"INSERT INTO Keyspace.Aluno(IdAluno, Nome, Sexo, Endereco, Cidade, UF) VALUES(i, {fake.name()}, {fake.random_element(elements=('M', 'F'))}, {fake.address()}, {fake.city()}, {fake.state_abbr()})")
        end_time = time.time()
        print("Insert time TABLE Aluno: " + str(start_time-end_time))
        insert_time = start_time-end_time

        #INSERT TABELA AlunoDetalhe
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"INSERT INTO Keyspace.AlunoDetalhe(IdAluno, DataNasc, RendaFamiliar, NotaEnem) VALUES(i, {fake.date_of_birth(minimum_age=18, maximum_age=30)}, {fake.random_int(min=1000, max=5000)}, {fake.random_int(min=0, max=1000)})")
        end_time = time.time()
        print("Insert time TABLE AlunoDetalhe: " + str(start_time-end_time))
        insert_time += start_time-end_time
        
        #INSERT TABELA AlunoCurso
        start_time = time.time()
        for i in range(int(sys.argv[2])):
           self.session.execute(f"INSERT INTO Keyspace.AlunoCurso(IdAluno, IdCurso, Curriculo, CreditosVencidos) VALUES(i, {fake.random_int(min=1, max=7)}, {fake.random_int(min=100001, max=199999)}, {fake.random_int(min=1, max=1000)})")
        end_time = time.time()
        print("Insert time TABLE AlunoCurso: " + str(start_time-end_time))
        insert_time += start_time-end_time
        
        #INSERT TABELA Curso
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"INSERT INTO Keyspace.Curso(IdCurso, Nome, Sigla, Titulacao, Campus) VALUES(i, {fake.random_element(elements='Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica')}, {fake.random_element(elements = 'CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM')}, {fake.random_element(elements = 'Bacharelado', 'Tecnologo', 'Licenciatura')}, {fake.random_element(elements = 'Alegrete', 'Bage', 'Caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja')})")
        end_time = time.time()
        print("Insert time TABLE Curso: " + str(start_time-end_time))
        insert_time += start_time-end_time

        insert_time = insert_time/4
        print("Media dos Inserts: " + str(insert_time))

    #UPDATE -----------------------------------------------------------
    def Update(self):

        #UPDATE TABELA Aluno
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"UPDATE Keyspace.Aluno SET Nome = {fake.name()}, Sexo = {fake.random_element(elements=('M', 'F'))}, Endereco = {fake.address()}, Cidade = {fake.city()}, UF = {fake.state_abbr()} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[2]))}")
        end_time = time.time()
        print("Update time TABLE Aluno: " + str(start_time-end_time))
        update_time = start_time-end_time

        #UPDATE TABELA AlunoDetalhe
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"UPDATE Keyspace.AlunoDetalhe SET  DataNasc = {fake.date_of_birth(minimum_age=18, maximum_age=30)}, RendaFamiliar = {fake.random_int(min=1000, max=5000)}, NotaEnem = {fake.random_int(min=0, max=1000)} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[2]))}")
        end_time = time.time()
        print("Update time TABLE AlunoDetalhe: " + str(start_time-end_time)) 
        update_time += start_time-end_time

        #UPDATE TABELA AlunoCurso
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"UPDATE Keyspace.AlunoCurso SET Curriculo = {fake.random_int(min=100001, max=199999)}, CreditosVencidos = {fake.random_int(min=1, max=1000)} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[2]))}")
        end_time = time.time()
        update_time += start_time-end_time
        print("Update time TABLE AlunoCurso: " + str(start_time-end_time))

        #UPDATE TABELA Curso
        start_time = time.time()
        for i in range(int(sys.argv[2])):
            self.session.execute(f"UPDATE Keyspace.Curso SET Nome = {fake.random_element(elements='Ciencia da Computacao','Engenharia Agricola', 'Engenharia Civil', 'Engenharia de Software', 'Engenharia de Telecomunicacoes', 'Engenharia Eletrica', 'Engenharia Mecanica')}, Sigla = {fake.random_element(elements = 'CC', 'EA', 'EC', 'ES', 'ET', 'EE', 'EM')}, Titulacao = {fake.random_element(elements = 'Bacharelado', 'Tecnologo', 'Licenciatura')}, Campus = {fake.random_element(elements = 'Alegrete', 'Bage', 'Caçapava do Sul','Dom Pedrito', 'Itaqui', 'Jaguarao','Santana do Livramento', 'Sao Borja')} WHERE IdAluno = {fake.random_int(min=1, max=int(sys.argv[2]))}")
        end_time = time.time()
        print("Update time TABLE Curso: " + str(start_time-end_time))
        update_time += start_time-end_time

        update_time = update_time/4
        print("Media dos Updates: " + str(start_time-end_time))

        self.cluster.shutdown()

    def main():
        exe = cass()
        exe.Insert()
        exe.Update()
    
    if __name__ == "__main__":
        main()


























