using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public interface IServiceConnector : IDisposable {
		void Close();

		IMessageProcessor Connect(IServiceAddress address, ServiceType type);
	}
}