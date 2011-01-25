using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	public abstract class TreeLeaf : ITreeNode {
		protected TreeLeaf() {
		}

		~TreeLeaf() {
			Dispose(false);
		}

		public abstract int Length { get; }

		public abstract int Capacity { get; }

		public abstract long Id { get; }

		public abstract long MemoryAmount { get; }

		protected virtual void Dispose(bool disposing) {
		}

		public abstract void SetLength(int value);

		public byte Read(int position) {
			byte[] buffer = new byte[1];
			Read(position, buffer, 0, 1);
			return buffer[0];
		}

		public abstract void Read(int position, byte[] buffer, int offset, int count);

		public void Write(int position, byte value) {
			byte[] buffer = new byte[1];
			buffer[0] = value;
			Write(position, buffer, 0, 1);
		}

		public abstract void Write(int position, byte[] buffer, int offset, int count);

		public abstract void WriteTo(IAreaWriter area);

		public abstract void Shift(int position, int offset);

		void IDisposable.Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}