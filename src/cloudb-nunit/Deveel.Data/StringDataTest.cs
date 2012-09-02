using System;
using System.IO;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StoreType.Memory)]
	public sealed class StringDataTest : ShardedDataTestBase {
		public StringDataTest(StoreType storeType) 
			: base(storeType) {
		}

		[Test]
		public void SimpleAppend() {
			IDataFile file = Transaction.GetFile(new Key(0, 0, 1), FileAccess.ReadWrite);
			Assert.IsNotNull(file);

			StringData data = new StringData(file);
			data.Append("test");
			Assert.AreNotEqual(0, file.Position);

			file.Position = 0;
			string s = data.Substring(0, 4);
			Assert.AreEqual("test", s);

			Commit();
		}

		[Test]
		public void IncrementalAppend() {
			IDataFile file = Transaction.GetFile(new Key(0, 0, 1), FileAccess.ReadWrite);
			Assert.IsNotNull(file);

			StringData data = new StringData(file);

			for (int i = 0; i < 500; i++) {
				StringBuilder sb = new StringBuilder();
				for (int j = 0; j < i; j++) {
					sb.Append(" ");
				}

				data.Append(sb.ToString());
			}

			file.Position = 0;

			int offset = 0;
			for (int i = 0; i < 500; i++) {
				StringBuilder sb = new StringBuilder();
				for (int j = 0; j < i; j++) {
					sb.Append(" ");
				}

				string s = data.Substring(offset, i);
				Assert.AreEqual(sb.ToString(), s);

				offset += i;
			}
		}
	}
}