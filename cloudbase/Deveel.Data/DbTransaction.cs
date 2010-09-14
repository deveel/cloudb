using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbTransaction : ITransaction {
		internal ITransaction Parent {
			get { throw new NotImplementedException(); }
		}
		
		internal void CheckValid() {
			//TODO:
		}
		
		internal void OnFileChanged(string fileName) {
			//TODO:
		}
		
		public void Dispose() {
			throw new NotImplementedException();
		}

		public DataFile GetFile(Key key, FileAccess access) {
			throw new NotImplementedException();
		}

		public void PreFetchKeys(Key[] keys) {
			throw new NotImplementedException();
		}
	}
}