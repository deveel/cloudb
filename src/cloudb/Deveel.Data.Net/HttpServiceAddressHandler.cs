using System;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class HttpServiceAddressHandler : IServiceAddressHandler {
		public bool CanHandle(Type type) {
			return type == typeof(HttpServiceAddress);
		}

		public int GetCode(Type type) {
			return type == typeof(HttpServiceAddress) ? 2 : -1;
		}

		public Type GetTypeFromCode(int code) {
			return code == 2 ? typeof(HttpServiceAddress) : null;
		}

		public IServiceAddress FromString(string s) {
			return HttpServiceAddress.Parse(s);
		}

		public IServiceAddress FromBytes(byte[] bytes) {
			BinaryReader reader = new BinaryReader(new MemoryStream(bytes));
			string host = reader.ReadString();
			int port = reader.ReadInt32();
			string path = reader.ReadString();
			string query = reader.ReadString();
			return new HttpServiceAddress(host, port, path, query);
		}

		public string ToString(IServiceAddress address) {
			return address.ToString();
		}

		public byte[] ToBytes(IServiceAddress address) {
			HttpServiceAddress httpAddress = (HttpServiceAddress) address;

			MemoryStream stream = new MemoryStream();
			BinaryWriter writer = new BinaryWriter(stream);
			writer.Write(httpAddress.Host);
			writer.Write(httpAddress.Port);
			writer.Write(httpAddress.Path);
			writer.Write(httpAddress.Query);
			return stream.ToArray();
		}
	}
}