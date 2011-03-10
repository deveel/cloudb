using System;

namespace Deveel.Data {
	public interface IDataFile {
		long Length { get; }

		long Position { get; set; }


		int Read(byte[] buffer, int offset, int count);

		void Write(byte[] buffer, int offset, int count);

		void SetLength(long value);

		void Shift(long offset);

		void Delete();

		void CopyTo(IDataFile destFile, long size);

		void ReplicateTo(IDataFile target);
	}
}