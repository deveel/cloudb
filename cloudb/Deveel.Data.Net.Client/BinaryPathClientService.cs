using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net.Client {
	public class BinaryPathClientService : PathClientService {
		public BinaryPathClientService(IServiceAddress address, TcpServiceAddress managerAddress, string password) 
			: base(address, managerAddress, new TcpServiceConnector(password)) {
		}

		private Thread pollingThread;
		private bool polling;
		private TcpListener listener;

		protected override string Type {
			get { return "rpc"; }
		}

		private void Poll() {
			polling = true;
			
			try {
				//TODO: INFO log ...

				while (polling) {
					Socket s;
					try {
						// The socket to run the service,
						s = listener.AcceptSocket();
						s.NoDelay = true;
						int cur_send_buf_size = s.SendBufferSize;
						if (cur_send_buf_size < 256 * 1024) {
							s.SendBufferSize = 256 * 1024;
						}

						//TODO: INFO log ...

						TcpConnection c = new TcpConnection(this);
						ThreadPool.QueueUserWorkItem(c.Work, s);
					} catch (IOException e) {
						//TODO: WARN log ...
					}
				}
			} catch(Exception e) {
				//TODO: WARN log ...
			}
		}

		protected override void OnInit() {
			try {
				TcpServiceAddress tcpAddress = (TcpServiceAddress)Address;
				IPEndPoint endPoint = tcpAddress.ToEndPoint();
				listener = new TcpListener(endPoint);
				listener.Server.ReceiveTimeout = 0;
				listener.Start(150);
				int curReceiveBufSize = listener.Server.ReceiveBufferSize;
				if (curReceiveBufSize < 256 * 1024) {
					listener.Server.ReceiveBufferSize = 256 * 1024;
				}
			} catch (IOException e) {
				//TODO: ERROR log ...
				return;
			}

			pollingThread = new Thread(Poll);
			pollingThread.Name = "TCP Path Service Polling";
			pollingThread.IsBackground = true;
			pollingThread.Start();
		}

		protected override void Dispose(bool disposing) {
			base.Dispose(disposing);

			if (disposing) {
				polling = false;
			}
		}

		#region TcpConnection

		private class TcpConnection {
			private readonly BinaryPathClientService service;

			public TcpConnection(BinaryPathClientService service) {
				this.service = service;
			}

			public void Work(object state) {
				Socket socket = (Socket) state;

				try {
					// Get as input and output stream on the sockets,
					NetworkStream socketStream = new NetworkStream(socket, FileAccess.ReadWrite);

					BinaryReader reader = new BinaryReader(new BufferedStream(socketStream, 4000), Encoding.UTF8);
					BinaryWriter writer = new BinaryWriter(new BufferedStream(socketStream, 4000), Encoding.UTF8);

					// service version (make it configurable)
					writer.Write(1);
					// authentication method (plain; make it configurable)
					writer.Write(1);

					//TODO: challenge for authentication ...

					while (true) {
						RequestType type = (RequestType) reader.ReadByte();
						int sz = reader.ReadInt32();
						StringBuilder sb = new StringBuilder(sz);
						for (int i = 0; i < sz; i++) {
							sb.Append(reader.ReadChar());
						}

						Dictionary<string,object> args = new Dictionary<string, object>();

						string pathName = sb.ToString();

						if (reader.ReadByte() == 1) {
							sz = reader.ReadInt32();
							sb = new StringBuilder(sz);
							for (int i = 0; i < sz; i++) {
								sb.Append(reader.ReadChar());
							}

							args[RequestMessage.ResourceIdName] = sb.ToString();
						}

						int tid = reader.ReadInt32();
						if (!String.IsNullOrEmpty(service.TransactionIdKey))
							args[service.TransactionIdKey] = tid;

						ResponseMessage response = service.HandleRequest(type, pathName, args, socketStream);

						// Write and flush the output message,
						service.MessageSerializer.Serialize(response, socketStream);
						socketStream.Flush();

					} // while (true)

				} catch (IOException e) {
					if (e is EndOfStreamException ||
					    (e.InnerException is SocketException &&
					     ((SocketException)e.InnerException).ErrorCode == (int)SocketError.ConnectionReset)) {
						// Ignore this one also,
					} else {
						Logger.Client.Error("IO Error in a connection");
					}
				} catch (SocketException e) {
					if (e.ErrorCode == (int)SocketError.ConnectionReset) {
						// Ignore connection reset messages,
					} else {
						Logger.Client.Error("Socket Error in a connection.");
					}
				} finally {
					// Make sure the socket is closed before we return from the thread,
					try {
						socket.Close();
					} catch (Exception e) {
						Logger.Client.Error("Error while closing a socket.", e);
					}
				}
			}
		}

		#endregion
	}
}