using CRMServico.DTO;
using System.Data;
using System.Text;
using System.Globalization;
using MySql.Data.MySqlClient;
using System.Text.Json;
using RabbitMQ.Client;

namespace CRMServico
{
	public sealed class WindowsBackgroundService : BackgroundService
    {  
        private readonly IConfiguration _configuration;
        private readonly Clientes _clientes;
        private readonly ILogger<WindowsBackgroundService> _logger;
        public WindowsBackgroundService(
            Clientes clientes,
			IConfiguration configuration,
			ILogger<WindowsBackgroundService> logger) => (_clientes, _logger, _configuration) = (clientes, logger, configuration);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                var conn = _configuration.GetConnectionString("ConnectionMySql");

				while (!stoppingToken.IsCancellationRequested)
                {
					_logger.LogInformation("Importando dados da planilha Excel para o BD MySQL: {time}", DateTimeOffset.Now);
					List<ClienteDTO> clientes = Clientes.ReadXls();
                    Excluir(conn);
                    foreach (var item in clientes)
                    {
						string porte = PorteEmpresa();
                        decimal fatAnual = FatBrutoAnul(porte);
                        bool ativo = AtivoInativo();
						_logger.LogInformation("{IdHtml} - {Nome Empresarial} ", item.IdHtml, item.NomeEmpresarial);
                        item.PorteEmpresa = porte;
						item.FatBrutoAnual = fatAnual;
                        item.Ativo = ativo;
						InsereCliente(item, conn);

						//
						var factory = new ConnectionFactory() { HostName = "localhost" };
						using (var connection = factory.CreateConnection())
						using (var channel = connection.CreateModel())
						{
							channel.QueueDeclare(queue: "Clientes Importados para BD MySQL",
												 durable: false,
												 exclusive: false,
												 autoDelete: false,
                            arguments: null);

							string json = JsonSerializer.Serialize(clientes);
							var body = Encoding.UTF8.GetBytes(json);

							channel.BasicPublish(exchange: "",
												 routingKey: "Clientes Importados para BD MySQL",
												 basicProperties: null,
												 body: body);

						}

						await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
					}
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Message}", ex.Message);
                Environment.Exit(1);
            }
        }

		private bool AtivoInativo()
		{
			int n = 1;
			int MAX = 2;
			char[] alphabet = { '0', '1' };
			Random random = new Random();
			string res = "";
			for (int i = 0; i < n; i++)
				res = res + alphabet[(int)(random.Next(0, MAX))];
            return res == "0" ? false : true;
		}
		private string PorteEmpresa()
        {			
			int n = 1;
			int MAX = 3;
			char[] alphabet = { 'G','M', 'P'};
			Random random = new Random();
			string res = "";
			for (int i = 0; i < n; i++)
				res = res + alphabet[(int)(random.Next(0, MAX))];

			return res;
        }

        private decimal FatBrutoAnul(string tpPorte)
        {
			CultureInfo culture = new CultureInfo("pt-BR");
			Random randNum = new Random();
            string result;
            decimal vlTotal;

			if (tpPorte == "P")
				result = ((long)randNum.Next(12000000, 99999999)).ToString();
			else if (tpPorte == "M")
				result = ((long)randNum.Next(100000000, 499999999)).ToString();
			else
				result = ((long)randNum.Next(500000000, 2147483647)).ToString();

			string v4 = result.Substring(result.Length - 2);
			string v5 = result.Substring(0, (result.Length - 2));
			string v6 = v5 + "," + v4;
			vlTotal = Convert.ToDecimal(v6, culture);
			return vlTotal;
		}
        public void Excluir(string ConnectionString)
        {
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                MySqlCommand command = new MySqlCommand("prc_cliente_del", connection);
                command.Connection.Open();
                command.CommandType = CommandType.StoredProcedure;
                command.ExecuteNonQuery();
                connection.Close();
            }
        }
		public void InsereCliente(ClienteDTO model, string ConnectionString)
		{
			List<ClienteDTO> results = new List<ClienteDTO>();
			int returnValue = 0;
			
			using (MySqlConnection connection = new MySqlConnection(ConnectionString))
			{
				MySqlCommand command = new MySqlCommand("prc_cliente_ins", connection);
				command.Connection.Open();				
				command.CommandType = CommandType.StoredProcedure;
				command.Parameters.AddWithValue("@IdHtml", model.IdHtml);
				command.Parameters["@IdHtml"].Direction = ParameterDirection.Input;
				command.Parameters.AddWithValue("@CodInterno", model.CodInterno);
				command.Parameters.AddWithValue("@CnpjParametro", model.CnpjParametro);
				command.Parameters.AddWithValue("@CnpjConsultado", model.CnpjConsultado);
				command.Parameters.AddWithValue("@CnpjNumInscricao", model.CnpjNumInscricao);
				command.Parameters.AddWithValue("@NomeEmpresarial", model.NomeEmpresarial);
				command.Parameters.AddWithValue("@NomeFantasia", model.NomeFantasia);
				command.Parameters.AddWithValue("@PorteEmpresa", model.PorteEmpresa);
				command.Parameters.AddWithValue("@FatBrutoAnual", model.FatBrutoAnual);
				command.Parameters.AddWithValue("@Ativo", model.Ativo);
				command.Parameters.AddWithValue("@Logradouro", model.Logradouro);
				command.Parameters.AddWithValue("@Numero", model.Numero);
				command.Parameters.AddWithValue("@Bairro", model.Bairro);
				command.Parameters.AddWithValue("@Complemento", model.Complemento);
				command.Parameters.AddWithValue("@CEP", model.CEP);
				command.Parameters.Add("@Id", MySqlDbType.Int32);
				command.Parameters["@Id"].Direction = ParameterDirection.Output;
				command.ExecuteNonQuery();
				returnValue = (Int32)command.Parameters["@Id"].Value;
				connection.Close();
			}
		}
		public void Insere(ClienteDTO model, string ConnectionString)
        {
            StringBuilder sb = new StringBuilder();
            //string ConnectionString = @"Server=ADRIANO_DELL\SQLEXPRESS;DataBase=DBCRM;Trusted_Connection=True;";
			List<ClienteDTO> results = new List<ClienteDTO>();
            //int returnValue = 0;
            sb.Append("insert into Clientes(");
            sb.Append("IdHtml, CodInterno ,CnpjParametro ,CnpjConsultado ,CnpjNumInscricao ,NomeEmpresarial ,NomeFantasia, DataImportacao, DataCadastro, PorteEmpresa, FatBrutoAnual, Ativo ");
            sb.Append(") values (");
            sb.Append("@pIdHtml, @pCodInterno ,@pCnpjParametro ,@pCnpjConsultado ,@pCnpjNumInscricao ,@pNomeEmpresarial ,@pNomeFantasia, @pDataImportacao, @pDataCadastro, @pPorteEmpresa, @pFatBrutoAnual, @pAtivo )");

            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                MySqlCommand command = new MySqlCommand(sb.ToString(), connection);
                command.Connection.Open();
                //command.CommandText = "insert into Clientes(NomeEmpresarial,NomeFantasia) values (" + model.NomeEmpresarial + "," + model.NomeFantasia + ")";
                command.CommandType = CommandType.Text;
                command.Parameters.AddWithValue("@pIdHtml", model.IdHtml);
                command.Parameters.AddWithValue("@pCodInterno", model.CodInterno);
                command.Parameters.AddWithValue("@pCnpjParametro", model.CnpjParametro);
                command.Parameters.AddWithValue("@pCnpjConsultado", model.CnpjConsultado);
                command.Parameters.AddWithValue("@pCnpjNumInscricao", model.CnpjNumInscricao);
                command.Parameters.AddWithValue("@pNomeEmpresarial", model.NomeEmpresarial);
                command.Parameters.AddWithValue("@pNomeFantasia", model.NomeFantasia);
                command.Parameters.AddWithValue("@pDataImportacao", DateTime.Now);
				command.Parameters.AddWithValue("@pDataCadastro", DateTime.Now);
				command.Parameters.AddWithValue("@pPorteEmpresa", model.PorteEmpresa);
				command.Parameters.AddWithValue("@pFatBrutoAnual", model.FatBrutoAnual);
				command.Parameters.AddWithValue("@pAtivo", model.Ativo);
				//command.Parameters.Add("@returnValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
				command.ExecuteNonQuery();
                //SqlDataReader reader = command.ExecuteReader();
                //while (reader.Read())
                //{
                //    results.Add(new ClienteModelView()
                //    {
                //        Id = (int)reader[0],
                //        NomeEmpresarial = (string)reader[1],
                //        NomeFantasia = (string)reader[2]
                //        //Email = (string)reader[3]
                //    });
                //}
                //reader.Close();
                //returnValue = (int)command.Parameters["@returnValue"].Value;
                connection.Close();
            }
        }
    }
}