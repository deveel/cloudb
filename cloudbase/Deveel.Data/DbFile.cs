using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	public sealed class DbFile : DataFile {
		private readonly DbTransaction transaction;
		private readonly string fileName;
		private readonly DataFile parent;
		
		private bool dirty;
		
		internal DbFile(DbTransaction transaction, string fileName, DataFile parent) {
			this.transaction = transaction;
			this.fileName = fileName;
			this.parent = parent;
		}
		
		public string Name {
			get { return fileName; }
		}

		public override long Position {
			get {
				transaction.CheckValid();
				return parent.Position;
			}
			set {
				transaction.CheckValid();
				parent.Position = value;
			}
		}
		
		public override long Length {
			get {
				transaction.CheckValid();
				return parent.Length;
			}
		}
		
		private void OnChanged() {
			if (dirty)
				return;
      
			transaction.OnFileChanged(fileName);
			dirty = true;
		}
		
		public override void Write(byte[] buffer, int offset, int count) {
			transaction.CheckValid();
			OnChanged();
			parent.Write(buffer, offset, count);
		}
		
		public override int Read(byte[] buffer, int offset, int count) {
			transaction.CheckValid();
			return parent.Read(buffer, offset, count);
		}
		
		public override void SetLength(long value) {
			transaction.CheckValid();
			OnChanged();
			parent.SetLength(value);
		}
		
		public override void Shift(long offset) {
			transaction.CheckValid();
			OnChanged();
			parent.Shift(offset);
		}
		
		public override void Delete() {
			transaction.CheckValid();
			OnChanged();
			parent.Delete();
		}
		
		public override void CopyTo(DataFile destFile, long size) {
			transaction.CheckValid();
			
			if (destFile is DbFile) {
				DbFile destDbFile = (DbFile) destFile;
				parent.CopyTo(destDbFile.parent, size);
				destDbFile.OnChanged();
			} else {
				parent.CopyTo(destFile, size);
			}
		}
	}
}