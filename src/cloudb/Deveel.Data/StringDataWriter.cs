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