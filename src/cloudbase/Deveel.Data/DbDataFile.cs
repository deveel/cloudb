using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbDataFile : DataFile {
		public override long Length {
			get { throw new NotImplementedException(); }
		}

		public override long Position {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override int Read(byte[] buffer, int offset, int count) {
			throw new NotImplementedException();
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotImplementedException();
		}

		public override void SetLength(long value) {
			throw new NotImplementedException();
		}

		public override void Shift(long offset) {
			throw new NotImplementedException();
		}

		public override void Delete() {
			throw new NotImplementedException();
		}

		public override void CopyTo(DataFile destFile, long size) {
			throw new NotImplementedException();
		}
	}
}