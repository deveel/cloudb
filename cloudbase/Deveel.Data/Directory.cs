using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	internal class Directory {
		private ITransaction transaction;
		private Key metaKey;
		private Key propertiesKey;
		private Key indexKey;

		private short itemKeyType;
		private int itemKeyPrimary;

		private IIndexedObjectComparer<string> comparer;

		private int version;

		public Directory(ITransaction transaction, Key metaKey, Key propertiesKey, Key indexKey, short itemKeyType, int itemKeyPrimary) {
			this.transaction = transaction;
			this.itemKeyPrimary = itemKeyPrimary;
			this.itemKeyType = itemKeyType;
			this.indexKey = indexKey;
			this.propertiesKey = propertiesKey;
			this.metaKey = metaKey;
		}

		private DataFile GetFile(Key key) {
			return transaction.GetFile(key, FileAccess.ReadWrite);
		}

		private Key GetKey(long id) {
			return new Key(itemKeyType, itemKeyPrimary, id);
		}

		private long UniqueId() {
			DataFile df = GetFile(metaKey);
			Properties pset = new Properties(df);
			long v = pset.GetValue("v", 16);
			pset.SetValue("v", v + 1);
			return v;
		}

		private string GetFileName(long id) {
			Key k = GetKey(id);
			DataFile df = GetFile(k);
			try {
				BinaryReader din = new BinaryReader(new DataFileStream(df));
				return din.ReadString();
			} catch (IOException e) {
				throw new ApplicationException(e.Message);
			}
		}

		public Key CreateFile(string fileName) {
			++version;

			Properties pset = new Properties(GetFile(propertiesKey));
			// Assert the item isn't already stored,
			if (pset.GetValue(fileName, -1L) != -1L)
				throw new ApplicationException("Item already exists: " + fileName);

			// Generate a unique identifier for the file name,
			long id = UniqueId();

			pset.SetValue(fileName, id);
			SortedIndex iset = new SortedIndex(GetFile(indexKey));
			iset.Insert(fileName, id, comparer);

			Key item_key = GetKey(id);
			DataFile df = GetFile(item_key);
			try {
				BinaryWriter dout = new BinaryWriter(new DataFileStream(df));
				dout.Write(fileName);
			} catch (IOException e) {
				throw new ApplicationException(e.Message);
			}

			return item_key;
		}

		#region ItemComparer

		private class ItemComparer : IIndexedObjectComparer<string> {
			private readonly Directory dir;

			public ItemComparer(Directory dir) {
				this.dir = dir;
			}

			public int Compare(long reference, string value) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}