using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public class ProxyServiceConnector : IServiceConnector {
		public ProxyServiceConnector(string net_password) {
			this.net_password = net_password;
		}

		private readonly String net_password;

		private BinaryReader pin;
		private BinaryWriter pout;
		private readonly object proxy_lock = new object();

		private string init_string = null;


		public void Connect(Stream stream) {
			pin = new BinaryReader(new BufferedStream(stream), Encoding.Unicode);
			pout = new BinaryWriter(new BufferedStream(stream), Encoding.Unicode);

			try {
				// Perform the handshake,
				long v = pin.ReadInt64();
				pout.Write(v);
				pout.Flush();
				init_string = pin.ReadString();
				pout.Write(net_password);
				pout.Flush();
			} catch (IOException e) {
				throw new Exception("IO Error", e);
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			Close();
		}

		#endregion

		#region Implementation of IServiceConnector

		public void Close() {
			try {
				lock (proxy_lock) {
					pout.Write('0');
					pout.Flush();
				}
				pin.Close();
				pout.Close();
			} catch (IOException) {
				//TODO: ERROR log ...
			} finally {
				init_string = null;
				pin = null;
				pout = null;
			}
		}

		public IMessageProcessor Connect(ServiceAddress address, ServiceType type) {
			return new MessageProcessor(this, address, type);
		}

		#endregion

		#region MessageProcessor

		private class MessageProcessor : IMessageProcessor {
			public MessageProcessor(ProxyServiceConnector connector, ServiceAddress address, ServiceType serviceType) {
				this.connector = connector;
				this.address = address;
				this.serviceType = serviceType;
			}

			private readonly ProxyServiceConnector connector;
			private readonly ServiceAddress address;
			private readonly ServiceType serviceType;

			#region Implementation of IMessageProcessor

			public MessageStream Process(MessageStream messageStream) {
				try {
					lock (connector.proxy_lock) {
						MessageStreamSerializer serializer = new MessageStreamSerializer();

						char code = '\0';
						if (serviceType == ServiceType.Admin)
							code = 'a';
						else if (serviceType == ServiceType.Block)
							code = 'b';
						else if (serviceType == ServiceType.Manager)
							code = 'm';
						else if (serviceType == ServiceType.Root)
							code = 'r';

						// Write the message.
						connector.pout.Write(code);
						address.WriteTo(connector.pout);
						serializer.Serialize(messageStream, connector.pout.BaseStream);
						connector.pout.Flush();

						return serializer.Deserialize(connector.pin.BaseStream);
					}
				} catch (IOException e) {
					// Probably caused because the proxy closed the connection when a
					// timeout was reached.
					throw new Exception("IO Error", e);
				}
			}

			#endregion
		}

		#endregion
	}
}