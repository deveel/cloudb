//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;

using Deveel.Data.Store;

namespace Deveel.Data {
	public abstract class TreeLeaf : ITreeNode {
		~TreeLeaf() {
			Dispose(false);
		}

		public abstract int Length { get; }

		public abstract int Capacity { get; }

		public abstract NodeId Id { get; }

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