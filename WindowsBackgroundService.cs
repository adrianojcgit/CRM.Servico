using CRM.Servico.DTO;
using System.Data;
using System.Text;
using System.Globalization;
using MySql.Data.MySqlClient;
using System.Text.Json;
using RabbitMQ.Client;
using System.Data.SqlClient;
using Newtonsoft.Json;

namespace CRM.Servico
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
                var connMySql = _configuration.GetConnectionString("ConnectionMySql");
                var connSql = _configuration.GetConnectionString("ConnectionSqlServer");

                while (!stoppingToken.IsCancellationRequested)
                {
					_logger.LogInformation("Importando dados da planilha Excel para o BD MySQL: {time}", DateTimeOffset.Now);
					List<ClienteDTO> clientes = Clientes.ReadXls();

                    ExcluirMySQL(connMySql);

                    string porte = "";
                    decimal fatAnual = 0;
                    bool ativo;

                    //MySQL
                    foreach (var item in clientes)
                    {
						porte = PorteEmpresa();
                        fatAnual = FatBrutoAnul(porte);
                        ativo = AtivoInativo();
						//_logger.LogInformation("{IdHtml} - {Nome Empresarial} ", item.IdHtml, item.NomeEmpresarial);
                        item.PorteEmpresa = porte;
						item.FatBrutoAnual = fatAnual;
                        item.Ativo = ativo;
						InsereClienteMySQL(item, connMySql);

                        //
                        var factoryMySQL = new ConnectionFactory() { HostName = "localhost" };
						using (var connection = factoryMySQL.CreateConnection())
						using (var channel = connection.CreateModel())
						{
                            channel.ConfirmSelect();
                            channel.BasicAcks += Channel_BasicAcks;
                            channel.BasicNacks += Channel_BasicNacks;
                            channel.BasicReturn += Channel_BasicReturn;


                            channel.QueueDeclare(queue: "clientes2",
												 durable: false,
												 exclusive: false,
												 autoDelete: false,
                                                 arguments: null);

                            string message = JsonConvert.SerializeObject(item);
                            Console.ForegroundColor = ConsoleColor.Magenta;
                            Console.WriteLine("Mensagem: " + message);
                            Console.ForegroundColor = ConsoleColor.Blue;
                            Console.WriteLine("*********************************************************************************************************************");
                            var body = Encoding.UTF8.GetBytes(message);

							channel.BasicPublish(exchange: "",
												 routingKey: "clientes2",
												 basicProperties: null,
												 body: body,
                                                 mandatory: true);
                            channel.WaitForConfirms(new TimeSpan(0, 0, 5));
						}

						await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);

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

        private static void Channel_BasicNacks(object sender, RabbitMQ.Client.Events.BasicNackEventArgs e)
        {
            Console.WriteLine($"{DateTime.UtcNow:o} -> Basic Nack");
        }

        private static void Channel_BasicAcks(object sender, RabbitMQ.Client.Events.BasicAckEventArgs e)
        {
            Console.WriteLine($"{DateTime.UtcNow:o} -> Basic Ack");
        }

        private static void Channel_BasicReturn(object sender, RabbitMQ.Client.Events.BasicReturnEventArgs e)
        {
            var message = Encoding.UTF8.GetString(e.Body.ToArray());
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{DateTime.UtcNow:o} -> Basic Return -> { message} ");
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
        public void ExcluirMySQL(string ConnectionString)
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
		public void InsereClienteMySQL(ClienteDTO model, string ConnectionString)
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
  
    }
}