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
using System.IO;
using System.Text;

namespace Deveel.Data {
	public sealed class StringDataWriter : TextWriter {
		private readonly StringData data;
		private long pos;

		public StringDataWriter(StringData data, long pos) {
			this.data = data;
			this.pos = pos;
		}

		public override Encoding Encoding {
			get { return Encoding.Unicode; }
		}

		public override void Write(char value) {
			data.SetPosition(pos);
			++pos;
			data.Write(value);
		}

		public override void Write(char[] buffer, int index, int count) {
			// Change the size if necessary
			long enda = data.Length;
			if (pos + count > enda)
				data.SetLength(pos + count);
			// Position and write
			data.SetPosition(pos);
			for (int i = index; i < index + count; ++i)
				data.Write(buffer[i]);
			pos += count;
		}
	}
}