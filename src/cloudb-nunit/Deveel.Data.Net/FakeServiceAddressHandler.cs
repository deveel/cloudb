using System;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class FakeServiceAddressHandler : IServiceAddressHandler {
		public bool CanHandle(Type type) {
			return type == typeof(FakeServiceAddress);
		}
		
		public int GetCode(Type type) {
			return type == typeof(FakeServiceAddress) ? 100 : -1;
		}
		
		public Type GetTypeFromCode(int code) {
			return code == 100 ? typeof(FakeServiceAddress) : null;
		}
		
		public IServiceAddress FromString(string s) {
			if (!s.StartsWith("@FAKE"))
				return null;
			s = s.Substring(5);
			s = s.Substring(0, s.Length - 1);
			return new FakeServiceAddress(s);
		}
		
		public IServiceAddress FromBytes(byte[] bytes) {
			string id = Encoding.ASCII.GetString(bytes);
			return new FakeServiceAddress(id);
		}
		
		public string ToString(IServiceAddress address) {
			return address.ToString();
		}
		
		public byte[] ToBytes(IServiceAddress address) {
			FakeServiceAddress fakeAddress = (FakeServiceAddress) address;
			return Encoding.ASCII.GetBytes(fakeAddress.Id);
		}
	}
}