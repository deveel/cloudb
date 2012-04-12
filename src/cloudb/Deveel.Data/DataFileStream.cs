using System;
using System.IO;

namespace Deveel.Data {
	public sealed class DataFileStream : Stream {
		public DataFileStream(IDataFile file, FileAccess access) {
			this.file = file;
			this.access = access;
		}

		public DataFileStream(IDataFile file)
			: this(file, FileAccess.ReadWrite) {
		}

		private readonly IDataFile file;
		private readonly FileAccess access;

		#region Overrides of Stream

		public override void Flush() {
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (!CanSeek)
				throw new InvalidOperationException("The current stream is not seekable.");

			if (origin == SeekOrigin.End)
				throw new NotSupportedException();

			if (origin == SeekOrigin.Current) {
				long p = file.Position;
				long s = file.Length;
				long to_skip = Math.Min(offset, s - p);
				file.Position = p + to_skip;
				return p + to_skip;
			}

			file.Position = offset;
			return offset;
		}

		public override void SetLength(long value) {
			file.SetLength(value);
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new InvalidOperationException("The current stream is not readable.");

			if (count == 0)
				return 0;

			long p = file.Position;
			long s = file.Length;
			// The amount to read, either the length of the array or the amount of
			// data left available, whichever is smaller.
			long to_read = Math.Min(count, s - p);

			if (to_read == 0)
				return 0;

			// Fill up the array
			int act_read = (int)to_read;
			return file.Read(buffer, offset, act_read);

		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new InvalidOperationException("The current stream is not writeable.");

			file.Write(buffer, offset, count);
		}

		public override bool CanRead {
			get { return (access & FileAccess.Read) != 0; }
		}

		public override bool CanSeek {
			get { return CanRead; }
		}

		public override bool CanWrite {
			get { return (access & FileAccess.Write) != 0; }
		}

		public override long Length {
			get { return file.Length; }
		}

		public override long Position {
			get { return file.Position; }
			set { file.Position = value; }
		}

		#endregion
	}
}