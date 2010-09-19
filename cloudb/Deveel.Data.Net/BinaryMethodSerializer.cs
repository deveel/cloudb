using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class BinaryMethodSerializer : IMethodSerializer {
		private static readonly Dictionary<byte, Type> typeCodes;

		static BinaryMethodSerializer() {
			typeCodes = new Dictionary<byte, Type>();
			typeCodes[0] = typeof(DBNull);

			typeCodes[1] = typeof(byte);
			typeCodes[2] = typeof(short);
			typeCodes[3] = typeof(int);
			typeCodes[4] = typeof(long);
			typeCodes[5] = typeof(float);
			typeCodes[6] = typeof(double);

			typeCodes[11] = typeof(DateTime);
			typeCodes[12] = typeof(TimeSpan);

			typeCodes[22] = typeof(char);
			typeCodes[23] = typeof(string);

			typeCodes[33] = typeof(bool);

			typeCodes[57] = typeof(Array);
		}

		private static Type GetType(byte code) {
			Type type;
			if (typeCodes.TryGetValue(code, out type))
				return type;
			return null;
		}

		public static byte GetCode(Type type) {
			foreach(KeyValuePair<byte, Type> pair in typeCodes) {
				if (pair.Value == type)
					return pair.Key;
			}

			throw new InvalidOperationException();
		}

		private static object ReadValue(BinaryReader reader, byte typeCode) {
			Type type = GetType(typeCode);
			if (type == typeof(DBNull))
				return null;
			if (type == typeof(bool))
				return reader.ReadBoolean();
			if (type == typeof(byte))
				return reader.ReadByte();
			if (type == typeof(short))
				return reader.ReadInt16();
			if (type == typeof(int))
				return reader.ReadInt32();
			if (type == typeof(long))
				return reader.ReadInt64();
			if (type == typeof(float))
				return reader.ReadSingle();
			if (type == typeof(double))
				return reader.ReadDouble();
			if (type == typeof(DateTime))
				return DateTime.FromBinary(reader.ReadInt64());

			if (type == typeof(char))
				return reader.ReadChar();

			if (type == typeof(string)) {
				int sz = reader.ReadInt32();
				StringBuilder sb = new StringBuilder(sz);
				for (int i = 0; i < sz; i++) {
					sb.Append(reader.ReadChar());
				}
				return sb.ToString();
			}

			// array
			if (type == typeof(Array)) {
				byte arrayTypeCode = reader.ReadByte();
				int sz = reader.ReadInt32();
				Type arrayType = GetType(arrayTypeCode);
				Array array = Array.CreateInstance(arrayType, sz);
				for (int i = 0; i < sz; i++) {
					object value = ReadValue(reader, arrayTypeCode);
					array.SetValue(value, i);
				}

				return array;
			}

			throw new FormatException();
		}

		private static MethodArgument ReadArgument(BinaryReader reader) {
			string argName = reader.ReadString();
			byte argType = reader.ReadByte();
			object value = ReadValue(reader, argType);

			MethodArgument argument = new MethodArgument(argName, value);

			int sz = reader.ReadInt32();

			for (int i = 0; i < sz; i++)
				argument.Children.Add(ReadArgument(reader));

			return argument;
		}

		public void DeserializeRequest(MethodRequest request, Stream input) {
			DeserializeRequest(request, new BinaryReader(input));
		}

		public void DeserializeRequest(MethodRequest request, BinaryReader reader) {
			int sz = reader.ReadInt32();
			for (int i = 0; i < sz; i++)
				request.Arguments.Add(ReadArgument(reader));

			int v = reader.ReadInt32();
			if (v != 8)
				throw new FormatException();
		}

		public void SerializeResponse(MethodResponse response, Stream output) {
			SerializeResponse(response, new BinaryWriter(new BufferedStream(output)));
		}

		public void SerializeResponse(MethodResponse response, BinaryWriter writer) {
			int sz = response.Arguments.Count;
			for (int i = 0; i < sz; i++) {
				MethodArgument argument = response.Arguments[i];
				WriteArgument(argument, writer);
			}

			writer.Write(8);
		}

		private static void WriteValue(object value, BinaryWriter writer) {
			Type valueType = value == null ? typeof(DBNull) : value.GetType();
			byte valueTypeCode = GetCode(valueType);
			writer.Write(valueTypeCode);

			if (value == null) {

			} else if (valueType == typeof(byte)) {
				writer.Write((byte)value);
			} else if (valueType == typeof(short)) {
				writer.Write((short)value);
			} else if (valueType == typeof(int)) {
				writer.Write((int)value);
			} else if (valueType == typeof(long)) {
				writer.Write((long)value);
			} else if (valueType == typeof(float)) {
				writer.Write((float)value);
			} else if (valueType == typeof(double)) {
				writer.Write((double)value);
			} else if (valueType == typeof(bool)) {
				writer.Write((bool)value);
			} else if (valueType == typeof(DateTime)) {
				value = ((DateTime)value).ToBinary();
				writer.Write((long)value);
			} else if (valueType == typeof(TimeSpan)) {
				value = ((TimeSpan)value).Ticks;
				writer.Write((long)value);
			} else if (valueType == typeof(string)) {
				string s = (string)value;
				int sz = s.Length;
				writer.Write(sz);

				for (int i = 0; i < sz; i++) {
					writer.Write(s[i]);
				}
			} else if (value is Array) {
				Array array = (Array)value;
				Type arrayType = array.GetType().GetElementType();
				byte arrayTypeCode = GetCode(arrayType);

				int sz = array.Length;
				writer.Write(arrayTypeCode);
				writer.Write(sz);

				for (int i = 0; i < sz; i++) {
					object item = array.GetValue(i);
					WriteValue(item, writer);
				}
			}
		}

		private static void WriteArgument(MethodArgument argument, BinaryWriter writer) {
			writer.Write(argument.Name);
			WriteValue(argument.Value, writer);
		}
	}
}