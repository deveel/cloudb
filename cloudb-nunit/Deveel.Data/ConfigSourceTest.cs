using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ConfigSourceTest {
		[Test]
		public void TestIn32() {
			ConfigSource config = new ConfigSource();
			config.SetValue("key", 32);

			Assert.AreEqual(32, config.GetInt32("key"));
		}

		[Test]
		public void TestInt64() {
			ConfigSource config = new ConfigSource();
			config.SetValue("key", 64L);

			Assert.AreEqual(64L, config.GetInt32("key"));
		}
		
		[Test]
		public void TestString() {
			ConfigSource config = new ConfigSource();
			config.SetValue("key", "value");

			Assert.AreEqual("value", config.GetInt32("key"));
		}
	}
}