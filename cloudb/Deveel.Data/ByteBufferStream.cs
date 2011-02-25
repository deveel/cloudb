using System;
using System.IO;

namespace Deveel.Data {
	public sealed class ByteBufferStream : Stream {
		private readonly ByteBuffer byteBuffer;

		public ByteBufferStream(ByteBuffer byteBuffer) {
			if (byteBuffer == null)
				throw new ArgumentNullException("byteBuffer");

			this.byteBuffer = byteBuffer;
		}

		public 

		public override void Flush() {
		}

		public override long Seek(long offset, SeekOrigin origin) {
			//TODO:
			throw new NotImplementedException();
		}

		public override void SetLength(long value) {
			if (value > byteBuffer.Length) {
				//TODO:
				throw new NotImplementedException();
			} else if (value < byteBuffer.Length) {
				//TODO:
				throw new NotImplementedException();
			}
		}

		public override int Read(byte[] buffer, int offset, int count) {
			byteBuffer.Read(buffer, offset, count);
			return count;
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new InvalidOperationException();
			
			byteBuffer.Write(buffer, offset, count);
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return !byteBuffer.IsReadOnly; }
		}

		public override long Length {
			get { return byteBuffer.Length; }
		}

		public override long Position {
			get { return byteBuffer.Position; }
			set { Seek(value, SeekOrigin.Begin); }
		}
	}
}