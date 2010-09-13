using System;

namespace Deveel.Data.Net {
	public interface IServiceAddressHandler {
		bool CanHandle(Type type);
		
		int GetCode(Type type);
		
		Type GetTypeFromCode(int code);
		
		IServiceAddress FromString(string s);
		
		IServiceAddress FromBytes(byte[] bytes);
		
		string ToString(IServiceAddress address);
		
		byte[] ToBytes(IServiceAddress address);
	}
}