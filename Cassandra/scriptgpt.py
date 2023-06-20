from cassandra.cluster import Cluster

# Conectando ao cluster do Cassandra
cluster = Cluster(['localhost'])  # substitua 'localhost' pelo endereço do seu cluster
session = cluster.connect()

# Criando o keyspace (banco de dados) e definindo o keyspace atual
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS exemplo
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
""")
session.set_keyspace('exemplo')

# Criando as tabelas
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
        rendafamiliar FLOAT,
        notaenem FLOAT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS alunocurso (
        idaluno INT,
        idcurso INT,
        curriculo TEXT,
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

# Operação de INSERT
session.execute("""
    INSERT INTO aluno (idaluno, nome, sexo, endereco, cidade, uf)
    VALUES (1, 'João', 'Masculino', 'Rua A', 'São Paulo', 'SP')
""")

session.execute("""
    INSERT INTO alunodetalhe (idaluno, datanasc, rendafamiliar, notaenem)
    VALUES (1, '1990-01-01', 3000.0, 650.0)
""")

session.execute("""
    INSERT INTO alunocurso (idaluno, idcurso, curriculo, creditosvencidos)
    VALUES (1, 1, 'Curriculo A', 10)
""")

session.execute("""
    INSERT INTO curso (idcurso, nome, sigla, titulacao, campus)
    VALUES (1, 'Engenharia', 'ENG', 'Bacharelado', 'Campus A')
""")

# Operação de UPDATE
session.execute("""
    UPDATE aluno
    SET endereco = 'Rua B'
    WHERE idaluno = 1
""")

# Operação de DELETE
session.execute("""
    DELETE FROM aluno
    WHERE idaluno = 1
""")

# Fechando a conexão com o cluster
cluster.shutdown()
