using System;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	/// <summary>
	/// Connects services between each others, exchanging messages
	/// through a given connection protocol.
	/// </summary>
	public interface IServiceConnector : IDisposable {
		/// <summary>
		/// Gets the instance of the <see cref="IMessageSerializer"/> used
		/// to serialize messages through the connection.
		/// </summary>
		IMessageSerializer MessageSerializer { get; set; }

		IServiceAuthenticator Authenticator { get; set; }

		/// <summary>
		/// Opens a connection with the service at the given address.
		/// </summary>
		/// <param name="address">The address of the service to connect to.</param>
		/// <param name="type">The type of service.</param>
		/// <returns>
		/// Returns and instance of <see cref="IMessageProcessor"/> that is used by
		/// a <see cref="IService"/> to send messages and retrieve responses.
		/// </returns>
		IMessageProcessor Connect(IServiceAddress address, ServiceType type);

		/// <summary>
		/// Closes the connector and every connection opened with <see cref="Connect"/>.
		/// </summary>
		void Close();
	}
}