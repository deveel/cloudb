using System;
using System.IO;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public class PropertiesConfigSourceTest {
		private static Stream GetStream(StringBuilder sb) {
			return GetStream(sb.ToString());
		}

		private static Stream GetStream(string s) {
			return new MemoryStream(Encoding.UTF8.GetBytes(s));
		}

		[Test]
		public void SimpleProperties() {
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("test=passed");
			sb.AppendLine("me=spaced value");

			ConfigSource source = new ConfigSource();
			source.LoadProperties(GetStream(sb));

			Assert.AreEqual("test", source.Keys[0]);
			Assert.AreEqual("me", source.Keys[1]);
			Assert.AreEqual("passed", source.GetString("test"));
			Assert.AreEqual("spaced value", source.GetString("me"));
		}

		[Test]
		public void StructuredProperties() {
			StringBuilder sb = new StringBuilder();
			sb.AppendLine("test.me = pass");
			sb.AppendLine("test.two = ed");
			sb.AppendLine("stress.it.to.the.max = 200");
			sb.AppendLine("stress.it.again = 23d");

			ConfigSource source = new ConfigSource();
			source.LoadProperties(GetStream(sb));

			Assert.AreEqual(2, source.GetChild("test").Keys.Length);
			Assert.AreEqual("ed", source.GetChild("test").GetString("two"));
		}
	}
}