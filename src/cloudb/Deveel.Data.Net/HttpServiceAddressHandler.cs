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