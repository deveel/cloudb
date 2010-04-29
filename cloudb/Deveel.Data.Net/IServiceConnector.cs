using System;

namespace Deveel.Data.Net {
	public interface IServiceConnector : IDisposable {
		void Close();

		IMessageProcessor Connect(ServiceAddress address, ServiceType type);
	}
}