using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public sealed class PathStramWriter {
		private readonly Stream outputStream;
		private readonly BinaryWriter output;

		public PathStramWriter(Stream outputStream, Encoding encoding){
			output = new BinaryWriter(outputStream, encoding);
			this.outputStream = outputStream;
		}

		public PathStramWriter(Stream outputStream)
			: this(outputStream, Encoding.Unicode) {
		}

		public Stream BaseStream {
			get { return outputStream; }
		}

		private void WriteValue(object value) {
			if (value == null)
				return;

			if (value is byte) {
				output.Write((byte)value);
			} else if (value is short) {
				output.Write((short)value);
			} else if (value is int) {
				output.Write((int)value);
			} else if (value is long) {
				output.Write((long)value);
			} else if (value is float) {
				output.Write((float)value);
			} else if (value is double) {
				output.Write((double)value);
			} else if (value is DateTime) {
				output.Write(UnixDateTime.ToUnixTimestamp((DateTime)value));
			} else if (value is String) {
				string s = (string) value;
				int sz = s.Length;
				output.Write(sz);
				for (int i = 0; i < sz; i++) {
					output.Write(s[i]);
				}
			} else if (value is Array) {
				//TODO:
			} else if (value is PathStruct) {
				PathStruct pathStruct = (PathStruct) value;
				string[] memberNames = pathStruct.MemberNames;
				int sz = pathStruct.MemberCount;

				output.Write(sz);
				for (int i = 0; i < sz; i++) {
					string memberName = memberNames[i];
					WriteValue(memberName);
					Write(pathStruct.GetValue(memberName));
				}
			} else {
				throw new ArgumentException("The value is not supported.");
			}
		}

		public void Write(PathValue value) {
			output.Write((byte)value.ValueType);
			WriteValue(value.Value);
		}

		public void Write(byte value) {
			Write(new PathValue(value));
		}

		public void Write(bool value) {
			Write(new PathValue(value));
		}

		public void Write(int value) {
			Write(new PathValue(value));
		}

		public void Write(long value) {
			Write(new PathValue(value));
		}

		public void Write(float value) {
			Write(new PathValue(value));
		}

		public void Write(double value) {
			Write(new PathValue(value));
		}

		public void Write(DateTime value) {
			Write(new PathValue(value));
		}

		public void Write(PathStruct value) {
			Write(new PathValue(value));
		}

		public void Write(string value) {
			Write(new PathValue(value));
		}

		public void Write(Array value) {
			Write(new PathValue(value));
		}
	}
}