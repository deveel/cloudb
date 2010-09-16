using System;

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
	}
}