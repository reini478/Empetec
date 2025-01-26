import express from 'express';
import oracledb from 'oracledb';

const app = express();
const PORT = 3000;

// Configurações de conexão com o banco Oracle
const dbConfig = {
  user: 'system', // Substitua pelo seu usuário
  password: 'senha', // Substitua pela sua senha
  connectString: 'localhost:1521/XE' // Formato padrão para Oracle XE
};

// Middleware para tratar JSON
app.use(express.json());

// Endpoint para consultar dados da tabela "sales"
app.get('/api/sales', async (req, res) => {
  let connection;

  try {
    // Conectando ao banco Oracle
    connection = await oracledb.getConnection(dbConfig);

    // Query em SQL
    const query = `SELECT * FROM sales`;

    const result = await connection.execute(query, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });

    // Retorno dos dados em formato JSON
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Erro ao consultar o banco:', err);
    res.status(500).json({ error: 'Erro ao consultar o banco de dados.' });
  } finally {
    // Encerrando a conexão
    if (connection) {
      try {
        await connection.close();
      } catch (closeErr) {
        console.error('Erro ao fechar a conexão:', closeErr);
      }
    }
  }
});

// Inicia o servidor
app.listen(PORT, () => {
  console.log(`API rodando em http://localhost:${PORT}`);
});
