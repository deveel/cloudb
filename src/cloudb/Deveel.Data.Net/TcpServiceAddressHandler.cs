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

namespace Deveel.Data.Net {
	public sealed class TcpServiceAddressHandler : IServiceAddressHandler {
		public bool CanHandle(Type type) {
			return type == typeof(TcpServiceAddress);
		}
		
		public int GetCode(Type type) {
			return (type == typeof(TcpServiceAddress)) ? 1 : -1;
		}
		
		public Type GetTypeFromCode(int code) {
			return code == 1 ? typeof(TcpServiceAddress) : null;
		}
		
		public IServiceAddress FromString(string s) {
			return TcpServiceAddress.Parse(s);
		}
		
		public IServiceAddress FromBytes(byte[] bytes) {
			short length = Util.ByteBuffer.ReadInt2(bytes, 0);
			byte[] address = new byte[length];
			Array.Copy(bytes, 2, address, 0, length);
			int port = Util.ByteBuffer.ReadInt4(bytes, length + 2);
			return new TcpServiceAddress(address, port);
		}
		
		public string ToString(IServiceAddress address) {
			return address.ToString();
		}
		
		public byte[] ToBytes(IServiceAddress address) {
			TcpServiceAddress tcpAddress = (TcpServiceAddress)address;
			
			int length = tcpAddress.IsIPv4 ? 4 : 16;
			byte[] buffer = new byte[length + 2 + 4];
			Util.ByteBuffer.WriteInt2((short)length, buffer, 0);
			Array.Copy(tcpAddress.Address, 0, buffer, 2, length);
			Util.ByteBuffer.WriteInt4(tcpAddress.Port, buffer, length + 2);
			return buffer;
		}
	}
}