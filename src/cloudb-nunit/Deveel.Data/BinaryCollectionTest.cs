using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Util;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StoreType.Memory)]
	[TestFixture(StoreType.FileSystem)]
	public sealed class BinaryCollectionTest : ShardedDataTestBase {
		public BinaryCollectionTest(StoreType storeType)
			: base(storeType) {
		}

		[Test]
		public void AddEmpties() {
			IDataFile df = Transaction.GetFile(new Key(0, 0, 1), FileAccess.ReadWrite);
			BinaryCollection collection = new BinaryCollection(df);

			for (int i = 0; i < 500; i += 2) {
				byte[] data = new byte[i];
				Assert.IsTrue(collection.Add(new Binary(data)));
			}
			for (int i = 499; i > 0; i -= 2) {
				byte[] data = new byte[i];
				Assert.IsTrue(collection.Add(new Binary(data)));
			}
		}

		[Test]
		public void CheckEmpties() {
			AddEmpties();

			IDataFile df = Transaction.GetFile(new Key(0, 0, 1), FileAccess.ReadWrite);
			BinaryCollection collection = new BinaryCollection(df);

			int sz = 0;
			foreach (Binary arr in collection) {
				Assert.AreEqual(sz, arr.Length);
				++sz;
			}
		}

		[Test]
		public void RemoveEmpties() {
			AddEmpties();

			IDataFile df = Transaction.GetFile(new Key(0, 0, 1), FileAccess.ReadWrite);
			BinaryCollection collection = new BinaryCollection(df);

			for (int i = 0; i < 500; i += 2) {
				byte[] data = new byte[i];
				Assert.IsTrue(collection.Remove(new Binary(data)));
			}
			for (int i = 499; i > 0; i -= 2) {
				byte[] data = new byte[i];
				Assert.IsTrue(collection.Remove(new Binary(data)));
			}

			// Check it's empty,
			Assert.IsTrue(collection.IsEmpty);

			// Check there's nothing to iterate,
			foreach (Binary arr in collection) {
				Assert.Fail("Erroneous elements found in set.");
			}
		}

		[Test]
		public void CheckProperties() {
			IDataFile df = Transaction.GetFile(new Key(0, 0, 2), FileAccess.ReadWrite);
			BinaryCollection collection = new BinaryCollection(df);

			Assert.AreEqual(0, collection.Count);
			Assert.IsTrue(collection.IsEmpty);

			int curCount = 0;

			byte[] buf = new byte[32];
			{
				for (int i = 0; i < 5000; ++i) {
					// Check the element count up to the 500th element.
					if (i < 500 && collection.Count != i) {
						Assert.Fail("Size report mismatch.");
					}
					if (collection.IsEmpty && i > 0) {
						Assert.Fail("Erroneous collection.IsEmpty");
					}

					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					collection.Add(new Binary(buf, 0, 16));
					++curCount;
				}
				// Check the size matches after 5000
				Assert.AreEqual(curCount, collection.Count);
			}

			{
				// Check element sizes and content when read back,
				foreach (Binary arr in collection) {
					BinaryReader reader = new BinaryReader(arr.GetInputStream());
					long v1 = reader.ReadInt64();
					long v2 = reader.ReadInt64();
					int v = reader.Read();
					// Should be 0 (end of stream),
					Assert.AreEqual(-1, v);
				}
			}

			long[] removeExtra = new long[] {100, 190, 120, 130, 111, 3000, 90, 299};
			long[] notRemoved = new long[] {21, 4000, 3222, 33, 101, 191, 189, 2000};

			{
				// Remove and check size,
				for (int i = 20; i >= 0; --i) {
					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					collection.Remove(new Binary(buf, 0, 16));
					--curCount;
				}

				// Check size,
				Assert.AreEqual(curCount, collection.Count);

				foreach (long i in removeExtra) {
					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					collection.Remove(new Binary(buf, 0, 16));
					--curCount;
				}

				// Check size,
				Assert.AreEqual(curCount, collection.Count);
			}

			// Check we can't find the removed elements,
			{
				foreach (long i in removeExtra) {
					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					Binary elem = new Binary(buf, 0, 16);
					Assert.IsFalse(collection.Contains(elem), "Found unexpected entries.");

					BinaryCollection tailSet = collection.Tail(elem);
					if (!tailSet.IsEmpty && tailSet.First.Equals(elem)) {
						Assert.Fail("Found unexpected entries.");
					}
				}
			}

			// Check we can find a sample of elements not removed,
			{
				foreach (long i in notRemoved) {
					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					Binary elem = new Binary(buf, 0, 16);
					Assert.IsTrue(collection.Contains(elem), "Collection missing expected element.");

					BinaryCollection tailSet = collection.Tail(elem);
					if (tailSet.IsEmpty || !tailSet.First.Equals(elem)) {
						Assert.Fail("Collection missing expected element.");
					}
				}
			}
		}

		[Test]
		public void EnumeratorCheck() {
			IDataFile df1 = Transaction.GetFile(new Key(1, 0, 5), FileAccess.ReadWrite);
			IDataFile df2 = Transaction.GetFile(new Key(2, 0, 5), FileAccess.ReadWrite);
			BinaryCollection collection1 = new BinaryCollection(df1);

			Random rnd = new Random(5);
			// Add some data,
			byte[] buf = new byte[300];
			{
				for (int i = 0; i < 100; ++i) {
					ByteBuffer.WriteInt8(i, buf, 0);
					ByteBuffer.WriteInt8((10 - i), buf, 8);
					// Add random sized records
					collection1.Add(new Binary(buf, 0, 16 + rnd.Next(284)));
				}
			}

			// Copy the data from df1,
			df1.CopyTo(df2, df1.Length);
			BinaryCollection collection2 = new BinaryCollection(df2);

			// Basic iterate over the set,
			{
				IEnumerator<Binary> i = collection1.GetEnumerator();
				long ci = 0;
				while (i.MoveNext()) {
					Binary barr = i.Current;
					BinaryReader din = new BinaryReader(barr.GetInputStream());
					long inI = din.ReadInt64();
					Assert.AreEqual(inI, ci);
					++ci;
				}
			}

			{
				IInteractiveEnumerator<Binary> i;
				// Iterate over the set and remove index 1, 3, 5, 7, etc
				i = (IInteractiveEnumerator<Binary>) collection1.GetEnumerator();
				long ci = 0;
				while (i.MoveNext()) {
					if ((ci % 2) == 1) {
						i.Remove();
					}
					++ci;
				}
				// And check,
				i = (IInteractiveEnumerator<Binary>) collection1.GetEnumerator();
				ci = 0;
				while (i.MoveNext()) {
					Binary barr = i.Current;
					BinaryReader din = new BinaryReader(barr.GetInputStream());
					long inI = din.ReadInt64();
					Assert.AreEqual(inI, ci);
					ci += 2;
				}
			}

			// Operations on set_2 (the copy)
			{
				IInteractiveEnumerator<Binary> i;
				// Iterate over the set and remove index 0, 2, 4, 6, etc
				i = (IInteractiveEnumerator<Binary>) collection2.GetEnumerator();
				long ci = 0;
				while (i.MoveNext()) {
					if ((ci % 2) == 0) {
						i.Remove();
					}
					++ci;
				}
				// And check,
				i = (IInteractiveEnumerator<Binary>) collection2.GetEnumerator();
				ci = 1;
				while (i.MoveNext()) {
					Binary barr = i.Current;
					BinaryReader din = new BinaryReader(barr.GetInputStream());
					long inI = din.ReadInt64();
					Assert.AreEqual(inI, ci);
					ci += 2;
				}
			}
		}
	}
}