import pandas as pd
import cx_Oracle

# Configurações de conexão com o banco Oracle
dsn_tns = cx_Oracle.makedsn('192.168.1.100', 1521, service_name='XE')  # Substituir pelos detalhes do banco
connection = cx_Oracle.connect(user='system', password='senha', dsn=dsn_tns)

# Função para normalizar valores (exemplo: escala entre 0 e 1)
def normalize_column(data):
    return (data - data.min()) / (data.max() - data.min())

# Caminho para o arquivo CSV
csv_file = 'sales_data.csv'

# Carregar os dados do CSV
data = pd.read_csv(csv_file)

# Verifica se o arquivo contém as colunas necessárias
required_columns = {'sale_id', 'customer_id', 'sale_date', 'sale_amount'}
if not required_columns.issubset(data.columns):
    raise ValueError(f"O arquivo CSV deve conter as colunas: {required_columns}")

# Transformação dos dados (exemplo: normalização da coluna 'sale_amount')
data['sale_amount'] = normalize_column(data['sale_amount'])

# Insere os dados na tabela Oracle
cursor = connection.cursor()

# Criação da tabela (caso ainda não exista)
cursor.execute("""
    DECLARE
        table_exists EXCEPTION;
        PRAGMA EXCEPTION_INIT(table_exists, -955); -- Código para tabela já existente
    BEGIN
        EXECUTE IMMEDIATE '
        CREATE TABLE sales (
            sale_id NUMBER PRIMARY KEY,
            customer_id NUMBER NOT NULL,
            sale_date DATE NOT NULL,
            sale_amount NUMBER
        )';
    EXCEPTION
        WHEN table_exists THEN
            NULL; -- Ignora o erro se a tabela já existir
    END;
""")

# Inserção dos dados no banco
for _, row in data.iterrows():
    cursor.execute("""
        INSERT INTO sales (sale_id, customer_id, sale_date, sale_amount)
        VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), :4)
    """, (int(row['sale_id']), int(row['customer_id']), row['sale_date'], float(row['sale_amount'])))

# Commit para salvar as alterações
connection.commit()

# Encerrar a conexão
cursor.close()
connection.close()

print("ETL concluído com sucesso.")