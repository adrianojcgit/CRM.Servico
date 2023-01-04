using CRMCloud;
using Microsoft.Extensions.Logging.Configuration;
using Microsoft.Extensions.Logging.EventLog;

internal class Program
{
	private static async Task Main(string[] args)
	{
		using IHost host = Host.CreateDefaultBuilder(args)
		.UseWindowsService(options =>
		{
			options.ServiceName = "CRMServico";
		})
		.ConfigureServices(services =>
		{
			LoggerProviderOptions.RegisterProviderOptions<EventLogSettings, EventLogLoggerProvider>(services);
			services.AddSingleton<Clientes>();
			services.AddHostedService<WindowsBackgroundService>();

		})
		.ConfigureLogging((context, logging) =>
		{
			logging.AddConfiguration(
				context.Configuration.GetSection("Logging"));
		})
		.Build();

		await host.RunAsync();
	}
}