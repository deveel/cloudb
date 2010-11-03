using System;
using System.IO;
using System.Text;

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
		
		[Test]
		public void LoadProperties() {
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("test=23");
			sb.AppendLine("test.foo=12");
			sb.AppendLine("test.bar=test");
			Stream input = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
			
			ConfigSource config = new ConfigSource();
			config.LoadProperties(input);
			
			Assert.AreEqual(1, config.Keys.Length);
			Assert.AreEqual("test", config.Keys[0]);
			Assert.AreEqual(2, config.GetChild("test").Keys.Length);
			Assert.AreEqual("foo", config.GetChild("test").Keys[0]);
			Assert.AreEqual("bar", config.GetChild("test").Keys[1]);
			Assert.AreEqual(12, config.GetChild("test").GetInt32("foo"));
			Assert.AreEqual("test", config.GetChild("test").GetString("bar"));
		}
	}
}