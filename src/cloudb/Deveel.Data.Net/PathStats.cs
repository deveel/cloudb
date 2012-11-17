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
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Deveel.Data.Net {
	public sealed class PathStats {
		private readonly Type pathType;
		private readonly Dictionary<string, object> stats;

		private const byte Null = 0;
		private const byte Byte = 1;
		private const byte Short = 2;
		private const byte Int = 4;
		private const byte Long = 8;
		private const byte Float = 9;
		private const byte Double = 10;
		private const byte DateTime = 11;
		private const byte String = 20;
		private const byte ByteArray = 30;
		private const byte Convertible = 31;

		public PathStats(Type pathType) {
			if (pathType == null)
				throw new ArgumentNullException("pathType");

			this.pathType = pathType;
			stats = new Dictionary<string, object>();
		}

		public Type PathType {
			get { return pathType; }
		}

		public T GetValue<T>(string key, T defaultValue) {
			object value;
			if (!stats.TryGetValue(key, out value))
				value = defaultValue;

			if (!typeof(T).IsInstanceOfType(value))
				value = Convert.ChangeType(value, typeof (T));

			return (T)value;
		}

		public T GetValue<T>(string key) {
			return GetValue(key, default(T));
		}

		public void SetValue<T>(string key, T value) {
			if (default(T).Equals(value)) {
				stats.Remove(key);
			} else {
				stats[key] = value;
			}
		}

		internal byte[] ToBinary() {
			MemoryStream stream = new MemoryStream(1024);
			BinaryWriter writer = new BinaryWriter(stream);

			writer.Write(pathType.AssemblyQualifiedName);

			int statCount = stats.Count;
			writer.Write(statCount);

			foreach (KeyValuePair<string, object> pair in stats) {
				writer.Write(pair.Key);

				object value = pair.Value;
				if (value == null) {
					writer.Write(Null);
				} else if (value is int) {
					writer.Write(Int);
					writer.Write((int)value);
				} else if (value is byte) {
					writer.Write(Byte);
					writer.Write((byte)value);
				} else if (value is long) {
					writer.Write(Long);
					writer.Write((long)value);
				} else if (value is short) {
					writer.Write(Short);
					writer.Write((short)value);
				} else if (value is float) {
					writer.Write(Float);
					writer.Write((float)value);
				} else if (value is double) {
					writer.Write(Double);
					writer.Write((double)value);
				} else if (value is string) {
					writer.Write(String);
					writer.Write((string)value);
				} else if (value is DateTime) {
					DateTime d = (DateTime) value;
					writer.Write(DateTime);
					writer.Write(d.ToUniversalTime().ToBinary());
				} else if (value is byte[]) {
					byte[] buffer = (byte[]) value;
					writer.Write(ByteArray);
					writer.Write(buffer.Length);
					writer.Write(buffer);
				} else if (Attribute.IsDefined(value.GetType(), typeof(SerializableAttribute))) {
					try {
						MemoryStream tmpStream = new MemoryStream();
						BinaryFormatter formatter = new BinaryFormatter();
						formatter.Serialize(tmpStream, value);
						tmpStream.Flush();
						byte[] buffer = tmpStream.ToArray();
						writer.Write(Convertible);
						writer.Write(buffer.Length);
						writer.Write(buffer);
					} catch (Exception e) {
						throw new InvalidOperationException("Cannot serialize " + pair.Key, e);
					}
				} else {
					throw new InvalidOperationException("The statistic '" + pair.Key + "' cannot be serialized.");
				}
			}

			return stream.ToArray();
		}

		internal static PathStats FromBinary(byte[] buffer) {
			MemoryStream stream = new MemoryStream(buffer);
			BinaryReader reader = new BinaryReader(stream);

			string typeName = reader.ReadString();
			Type type = Type.GetType(typeName, false, true);

			PathStats stats = new PathStats(type);

			int sz = reader.ReadInt32();
			for (int i = 0; i < sz; i++) {
				string key = reader.ReadString();

				byte valueKind = reader.ReadByte();
				object value;

				if (valueKind == Null) {
					value = null;
				} else if (valueKind == Byte) {
					value = reader.ReadByte();
				} else if (valueKind == Short) {
					value = reader.ReadInt16();
				} else if (valueKind == Int) {
					value = reader.ReadInt32();
				} else if (valueKind == Long) {
					value = reader.ReadInt64();
				} else if (valueKind == Float) {
					value = reader.ReadSingle();
				} else if (valueKind == Double) {
					value = reader.ReadDouble();
				} else if (valueKind == String) {
					value = reader.ReadString();
				} else if (valueKind == DateTime) {
					long v = reader.ReadInt64();
					value = System.DateTime.FromBinary(v);
				} else if (valueKind == ByteArray) {
					int len = reader.ReadInt32();
					byte[] b = new byte[len];
					reader.Read(b, 0, len);
					value = b;
				} else if (valueKind == Convertible) {
					int len = reader.ReadInt32();
					byte[] b = new byte[len];
					reader.Read(b, 0, len);
					MemoryStream tmpStream = new MemoryStream(b);
					BinaryFormatter formatter = new BinaryFormatter();
					value = formatter.Deserialize(tmpStream);
				} else {
					throw new InvalidOperationException("Unrecognized value kind " + valueKind);
				}

				stats.stats.Add(key, value);
			}

			return stats;
		}
	}
}