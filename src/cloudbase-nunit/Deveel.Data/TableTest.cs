using System;
using System.Globalization;

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
				DbTable table = transaction.GetTable("test_table");
				table.AddColumn("col1");
				table.AddColumn("col2");
				table.AddIndex("col1");

				transaction.Commit();
			}

			using (DbTransaction transaction = Session.CreateTransaction()) {
				Assert.AreEqual(1, transaction.TableCount);

				DbTable table = transaction.GetTable("test_table");
				Assert.IsNotNull(table);
				Assert.AreEqual(2, table.ColumnCount);
				Assert.AreEqual("col1", table.ColumnNames[0]);
				Assert.AreEqual("col2", table.ColumnNames[1]);
			}
		}

		[Test]
		public void CreateAndPopulateTable500() {
			CreateTable();

			DateTime start;
			DateTime end;

			using (DbTransaction transaction = Session.CreateTransaction()) {
				DbTable table = transaction.GetTable("test_table");

				start = DateTime.Now;

				for (int i = 0; i < 500; i++) {
					table.BeginInsert();
					table.SetValue("col1", i.ToString(CultureInfo.InvariantCulture));
					table.SetValue("col2", String.Format("val.{0}", i));
					table.Complete();
				}

				transaction.Commit();

				end = DateTime.Now;
			}

			Console.Out.WriteLine("Time took for 500 inserts: {0}ms", (end - start).TotalSeconds);

			using (DbTransaction transaction = Session.CreateTransaction()) {
				DbTable table = transaction.GetTable("test_table");

				Assert.AreEqual(500, table.RowCount);

				int i = 0;
				foreach (DbRow row in table) {
					Assert.AreEqual(i.ToString(CultureInfo.InvariantCulture), row["col1"]);
					Assert.AreEqual(String.Format("val.{0}", i), row["col2"]);
					i++;
				}
			}
		}

		[Test]
		public void CreatePopulateAndEmptyTable() {
			CreateAndPopulateTable500();

			using (DbTransaction transaction = Session.CreateTransaction()) {
				DbTable table = transaction.GetTable("test_table");

				Assert.AreEqual(500, table.RowCount);

				Assert.IsTrue(table.Empty());
			}
		}
	}
}