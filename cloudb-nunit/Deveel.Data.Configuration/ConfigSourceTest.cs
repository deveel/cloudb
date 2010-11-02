using System;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
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

			Assert.AreEqual("value", config.GetString("key"));
		}

		[Test]
		public void AddChild() {
			ConfigSource config = new ConfigSource();
			ConfigSource child = config.AddChild("test");
			Assert.IsNotNull(child);
			Assert.AreEqual(1, config.ChildCount);
			Assert.AreEqual("test", child.Name);
		}

		[Test]
		public void AddSubChild() {
			ConfigSource config = new ConfigSource();
			ConfigSource child = config.AddChild("test.child");
			Assert.IsNotNull(child);
			Assert.AreEqual(1, config.ChildCount);
			Assert.AreEqual("child", child.Name);
			Assert.IsNotNull(child.Parent);
			Assert.AreEqual("test", child.Parent.Name);
		}

		[Test]
		public void GetChildValue() {
			ConfigSource config = new ConfigSource();
			ConfigSource child = config.AddChild("test.child");
			child.SetValue("value", 32);

			int value = config.GetInt32("test.child.value");
			Assert.AreEqual(32, value);
		}
	}
}