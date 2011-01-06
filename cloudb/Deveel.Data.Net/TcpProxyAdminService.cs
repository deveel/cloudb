using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class TcpProxyAdminService : Service {
		private readonly IPAddress address;
		private readonly int port;

		private TcpListener listener;
		private bool polling;
		private readonly Thread pollingThread;

		private IMessageSerializer serializer;

		public TcpProxyAdminService(IPAddress address, int port) {
			this.address = address;
			this.port = port;

			pollingThread = new Thread(Poll);
			pollingThread.IsBackground = true;
		}

		public override ServiceType ServiceType {
			get { return ServiceType.Admin; }
		}

		public IMessageSerializer MessageSerializer {
			get {
				if (serializer == null)
					serializer = new BinaryRpcMessageSerializer();
				return serializer;
			}
			set { serializer = value; }
		}

		private void Poll() {
			while (polling) {
				try {
					if (!listener.Pending()) {
						Thread.Sleep(500);
						continue;
					}

					Socket socket = listener.AcceptSocket();
					ProxyConnection conn = new ProxyConnection(this);
					ThreadPool.QueueUserWorkItem(conn.Process, socket);
				} catch(Exception e) {
					Logger.Warning("Socket Error while processing a proxy connection.", e);
				}
			}
		}

		protected override IMessageProcessor CreateProcessor() {
			return null;
		}

		protected override void OnStart() {
			try {
				listener = new TcpListener(new IPEndPoint(address, port));
				listener.Server.SendTimeout = 0;
				listener.Start(150);

				pollingThread.Start();
			} catch(Exception e) {
				Logger.Error("Error while starting the proxy server.", e);
				return;
			}
		}

		protected override void OnStop() {
			polling = false;
			if (listener != null)
				listener.Stop();
		}

		#region ProxyConnection

		private class ProxyConnection {
			private readonly TcpProxyAdminService service;

			public ProxyConnection(TcpProxyAdminService service) {
				this.service = service;
			}

			public void Process(object state) {
				Socket socket = (Socket)state;

				Stream socket_in = new NetworkStream(socket, FileAccess.Read);
				Stream socket_out = new NetworkStream(socket, FileAccess.Write);
				try {
					// 30 minute timeout on proxy connections,
					socket.SendTimeout = 30*60*1000;

					// Wrap the input stream in a data and buffered input stream,
					BufferedStream bin = new BufferedStream(socket_in, 1024);
					BinaryReader din = new BinaryReader(bin);

					// Wrap the output stream in a data and buffered output stream,
					BufferedStream bout = new BufferedStream(socket_out, 1024);
					BinaryWriter dout = new BinaryWriter(bout);

					// Perform the handshake,
					DateTime systemtime = DateTime.Now;
					dout.Write(systemtime.ToUniversalTime().ToBinary());
					dout.Flush();
					long back = din.ReadInt64();
					if (systemtime.ToUniversalTime().ToBinary() != back) {
						throw new IOException("Bad protocol request");
					}
					dout.Write("CloudB Proxy Service");
					dout.Flush();
					string net_password = din.ReadString();

					// The connector to proxy commands via,
					TcpServiceConnector connector = new TcpServiceConnector(net_password);

					// The rest of the communication will be command requests;
					while (true) {

						// Read the command,
						char command = din.ReadChar();
						if (command == '0') {
							// Close connection if we receive a '0' command char
							dout.Close();
							din.Close();
							return;
						}

						int addressCode = din.ReadInt32();
						Type addressType = ServiceAddresses.GetAddressType(addressCode);
						if (addressType == null || addressType != typeof(TcpServiceAddress))
							throw new ApplicationException("Invalid address type.");

						int addressLength = din.ReadInt32();
						byte[] addressBytes = new byte[addressLength];
						din.Read(addressBytes, 0, addressLength);

						IServiceAddressHandler handler = ServiceAddresses.GetHandler(addressType);
						TcpServiceAddress address = (TcpServiceAddress) handler.FromBytes(addressBytes);
						RequestMessage request = (RequestMessage) service.MessageSerializer.Deserialize(din.BaseStream, MessageType.Request);

						Message response;

						// Proxy the command over the network,
						if (command == 'a') {
							response = connector.Connect(address, ServiceType.Admin).Process(request);
						} else if (command == 'b') {
							response = connector.Connect(address, ServiceType.Block).Process(request);
						} else if (command == 'm') {
							response = connector.Connect(address, ServiceType.Manager).Process(request);
						} else if (command == 'r') {
							response = connector.Connect(address, ServiceType.Root).Process(request);
						} else {
							throw new IOException("Unknown command to proxy: " + command);
						}

						// Return the result,
						service.MessageSerializer.Serialize(response, dout.BaseStream);
						dout.Flush();

					}
				} catch(SocketException e) {
					if (e.ErrorCode == (int)SocketError.ConnectionReset) {
						// Ignore connection reset messages,
					}
				} catch (IOException e) {
					if (e is EndOfStreamException) {
						// Ignore this one oo,
					} else {
						service.Logger.Error("IO Error during connection input", e);
					}
				} finally {
					// Make sure the socket is closed before we return from the thread,
					try {
						socket.Close();
					} catch (IOException e) {
						service.Logger.Error("IO Error on connection close", e);
					}
				}
			}
		}

		#endregion
	}
}