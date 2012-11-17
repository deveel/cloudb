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