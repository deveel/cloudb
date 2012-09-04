using System;

namespace Deveel.Data {
	[Trusted]
	public sealed class DbFile : IDataFile {
		private readonly DbTransaction transaction;
		private readonly string fileName;

		private readonly IDataFile parent;

		private bool changed;

		internal DbFile(DbTransaction transaction, string fileName, IDataFile parent) {
			this.transaction = transaction;
			this.fileName = fileName;
			this.parent = parent;
		}

		public long Length {
			get {
				transaction.CheckValid();
				return parent.Length;
			}
		}
		
		public long Position {
			get {
				transaction.CheckValid();
				return parent.Position;
			} 
			set {
				transaction.CheckValid();
				parent.Position = value;
			}
		}

		public string Name {
			get { return fileName; }
		}

		private void LogChange() {
			if (!changed) {
				transaction.LogFileChange(Name);
				changed = true;
			}
		}

		public int Read(byte[] buffer, int offset, int count) {
			transaction.CheckValid();
			return parent.Read(buffer, offset, count);
		}

		public void SetLength(long value) {
			transaction.CheckValid();
			LogChange();
			parent.SetLength(value);
		}

		public void Shift(long offset) {
			transaction.CheckValid();
			LogChange();
			parent.Shift(offset);
		}

		public void Delete() {
			transaction.CheckValid();
			LogChange();
			parent.Delete();
		}

		public void Write(byte[] buffer, int offset, int count) {
			transaction.CheckValid();
			LogChange();
			parent.Write(buffer, offset, count);
		}

		public void CopyTo(IDataFile destFile, long size) {
			transaction.CheckValid();
			if (destFile is DbFile) {
				DbFile targetFile = (DbFile) destFile;
				parent.CopyTo(targetFile.parent, size);
				targetFile.LogChange();
			} else {
				parent.CopyTo(destFile, size);
			}
		}

		public void CopyFrom(IDataFile sourceFile, long size) {
			throw new NotImplementedException();
		}

		public void ReplicateTo(IDataFile destFile) {
			transaction.CheckValid();
			if (destFile is DbFile) {
				DbFile targetFile = (DbFile) destFile;
				parent.ReplicateTo(targetFile.parent);
				targetFile.LogChange();
			} else {
				parent.ReplicateTo(destFile);
			}
		}

		public void ReplicateFrom(IDataFile sourceFile) {
			throw new NotImplementedException();
		}
	}
}