using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Client {
	public sealed class PathStreamReader {
		private readonly Stream inputStream;
		private readonly BinaryReader input;

		public PathStreamReader(Stream inputStream)
			: this(inputStream, Encoding.Unicode) {
		}

		public PathStreamReader(Stream inputStream, Encoding encoding) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");
			if (!inputStream.CanRead)
				throw new ArgumentException("The input stream is not readable.");

			this.inputStream = inputStream;
			input = new BinaryReader(inputStream, encoding);
		}

		public Stream BaseStream {
			get { return inputStream; }
		}

		private PathValueType ReadType(PathValueType expected) {
			PathValueType type = (PathValueType) input.ReadByte();

			if (type != expected)
				throw new InvalidOperationException("The type '" + type + "' read does not match the type '" + expected +
				                                    "' that was expected.");

			return type;
		}

		private PathValue ReadValue(PathValueType type) {
			if (type == PathValueType.Null)
				return PathValue.Null;

			if (type == PathValueType.Boolean)
				return new PathValue(input.ReadBoolean());

			if (type == PathValueType.Byte)
				return new PathValue(input.ReadByte());
			if (type == PathValueType.Int16)
				return new PathValue(input.ReadInt16());
			if (type == PathValueType.Int32)
				return new PathValue(input.ReadInt32());
			if (type == PathValueType.Int64)
				return new PathValue(input.ReadInt64());
			if (type == PathValueType.Single)
				return new PathValue(input.ReadSingle());
			if (type == PathValueType.Double)
				return new PathValue(input.ReadDouble());
			
			if (type == PathValueType.String) {
				int length = input.ReadInt32();
				StringBuilder sb = new StringBuilder(length);
				for (int i = 0; i < length; i++) {
					sb.Append(input.ReadChar());
				}

				return new PathValue(sb.ToString());
			}

			if (type == PathValueType.DateTime)
				return new PathValue(UnixDateTime.ToDateTime(input.ReadInt64()));

			if (type == PathValueType.Struct) {
				int memberCount = input.ReadInt32();
				PathStruct pathStruct = new PathStruct();
				for (int i = 0; i < memberCount; i++) {
					string memberName = ReadValue(PathValueType.String);
					PathValue value = ReadValue();
					pathStruct.SetValue(memberName, value);
				}

				return new PathValue(pathStruct);
			}

			if (type == PathValueType.Array) {
				PathValueType elementType = (PathValueType) input.ReadByte();
				int length = input.ReadInt32();
				PathValue array = PathValue.CreateArray(elementType, length);

				for (int i = 0; i < length; i++) {
					PathValue value = ReadValue(elementType);
					array.SetValue(i, value.Value);
				}

				return array;
			}

			throw new NotSupportedException();
		}

		public PathValue ReadValue() {
			PathValueType type = (PathValueType) input.ReadByte();
			return ReadValue(type);
		}

		public PathValue ReadBoolean() {
			return ReadValue(ReadType(PathValueType.Boolean));
		}

		public PathValue ReadInt16() {
			return ReadValue(ReadType(PathValueType.Int16));
		}

		public PathValue ReadInt32() {
			return ReadValue(ReadType(PathValueType.Int32));
		}

		public PathValue ReadInt64() {
			return ReadValue(ReadType(PathValueType.Int64));
		}

		public PathValue ReadSingle() {
			return ReadValue(ReadType(PathValueType.Single));
		}

		public PathValue ReadDouble() {
			return ReadValue(ReadType(PathValueType.Double));
		}

		public PathValue ReadString() {
			return ReadValue(ReadType(PathValueType.String));
		}

		public PathValue ReadDateTime() {
			return ReadValue(ReadType(PathValueType.DateTime));
		}

		public PathValue ReadStruct() {
			return ReadValue(ReadType(PathValueType.Struct));
		}

		public PathValue ReadArray() {
			return ReadValue(ReadType(PathValueType.Array));
		}
	}
}