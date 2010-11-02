using System;

using Deveel.Data.Configuration;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public class LoggerTest {
		[Test]
		public void ConfigureDefaultDebuggerTest() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger", "default");

			LogManager.Init(config);
			ILogger logger = LogManager.GetLogger("default");
			Assert.IsInstanceOf(typeof(Logger), logger);
			Assert.IsInstanceOf(typeof(DefaultLogger), ((Logger)logger).BaseLogger);
		}
		
		[Test]
		public void ConfigureSimpleConsoleLogger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger", "simple-console");
			
			LogManager.Init(config);
			ILogger logger = LogManager.GetLogger("simple-console");
			Assert.IsInstanceOf(typeof(Logger), logger);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), ((Logger)logger).BaseLogger);
			
			((Logger)logger).Info("Printing a log message");
		}
		
		[Test]
		public void ConfigureNetworkLogger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("network_log_type", "simple-console");
			
			LogManager.Init(config);
			ILogger logger = LogManager.NetworkLogger;
			Assert.IsInstanceOf(typeof(Logger), logger);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), ((Logger)logger).BaseLogger);
		}
	}
}