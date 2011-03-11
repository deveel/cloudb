using System;

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
			return (s == "@FAKE@") ? new FakeServiceAddress() : null;
		}
		
		public IServiceAddress FromBytes(byte[] bytes) {
			return new FakeServiceAddress();
		}
		
		public string ToString(IServiceAddress address) {
			return "@FAKE@";
		}
		
		public byte[] ToBytes(IServiceAddress address) {
			return new byte[0];
		}
	}
}