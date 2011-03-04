using System;

using Deveel.Data.Configuration;

using NUnit.Framework;

namespace Deveel.Data.Diagnostics {
	[TestFixture]
	public class LoggerTest {
		[TearDown]
		public void TearDown() {
			Logger.Clear();
		}

		[Test]
		public void ConfigureDefaultDebugger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger.foo.type", "default");

			Logger.Init(config);
			Logger logger = Logger.GetLogger("foo");
			Assert.IsInstanceOf(typeof(DefaultLogger), logger.BaseLogger);
		}
		
		[Test]
		public void ConfigureSimpleConsoleLogger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger.foo.type", "simple-console");
			
			Logger.Init(config);
			Logger logger = Logger.GetLogger("foo");
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), logger.BaseLogger);
			
			logger.Info("Printing a log message");
		}
		
		[Test]
		public void ConfigureNetworkLogger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger.network.type", "simple-console");
			
			Logger.Init(config);
			Logger logger = Logger.Network;
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), logger.BaseLogger);
		}
		
		[Test]
		public void CompositeConsoleLogger() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger.composite.type", "composite");
			config.SetValue("logger.composite.1.type", "simple-console");
			config.SetValue("logger.composite.2.type", "simple-console");
			
			Logger.Init(config);
			Logger logger = Logger.GetLogger("composite");
			Assert.IsInstanceOf(typeof(CompositeLogger), logger.BaseLogger);
			
			logger.Info("Print a simple message that must be written in the console twice");
		}

		[Test]
		public void DefaultWithoutExplicitSpec() {
			ConfigSource config = new ConfigSource();
			config.SetValue("logger.format", "[{Time}] - {Source} - {Message]");

			Logger.Init(config);
			Logger logger = Logger.GetLogger();
			Assert.IsInstanceOf(typeof(DefaultLogger), logger.BaseLogger);
			Assert.AreEqual("[{Time}] - {Source} - {Message]", logger.Config.GetString("format"));
		}
	}
}