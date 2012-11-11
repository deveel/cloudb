using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StoreType.Memory)]
	public sealed class TableTest : CloudBaseTestBase {
		public TableTest(StoreType storeType) 
			: base(storeType) {
		}

		[Test]
		public void CreateTable() {
			using (DbTransaction transaction = Session.CreateTransaction()) {

				Assert.AreEqual(0, transaction.TableCount);

				transaction.CreateTable("test_table");

				Assert.AreEqual(1, transaction.TableCount);
			}
		}
	}
}