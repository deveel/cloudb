using System;

using Deveel.Data.Net.Client;
using Deveel.Data.Net.Serialization;
using Deveel.Data.Net.Security;

namespace Deveel.Data.Net {
	public interface IServiceConnector : IDisposable {
		IMessageSerializer MessageSerializer { get; set; }

		IAuthenticator Authenticator { get; set; }
		
		
		void Close();

		IMessageProcessor Connect(IServiceAddress address, ServiceType type);
	}
}