using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class TcpAdminService : AdminService {
		private bool polling;
		private TcpListener listener;
		private List<TcpConnection> connections;
		private IMessageSerializer serializer;

		public TcpAdminService(IAdminServiceDelegator delegator, IPAddress address, int port, string password)
			: this(delegator, new TcpServiceAddress(address, port),  password) {
		}
		
		public TcpAdminService(IAdminServiceDelegator delegator, IPAddress address, string password)
			: this(delegator, address, TcpServiceAddress.DefaultPort, password) {
		}
		
		public TcpAdminService(IAdminServiceDelegator delegator, TcpServiceAddress address, string password)
			: base(address, new TcpServiceConnector(password), delegator) {
		}

		public IMessageSerializer MessageSerializer {
			get {
				if (serializer == null)
					serializer = new BinaryRpcMessageSerializer();
				return serializer;
			}
			set { serializer = value; }
		}
		
		public bool IsListening {
			get { return polling; }
		}

		private void Poll() {
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

						// Make sure this ip address is allowed,
						IPAddress ipAddress = ((IPEndPoint)s.RemoteEndPoint).Address;

						//TODO: INFO log ...

						if (IPAddress.IsLoopback(ipAddress) || 
							IsAddressAllowed(ipAddress.ToString())) {
							// Dispatch the connection to the thread pool,
							TcpConnection c = new TcpConnection(this, s);
							if (connections == null)
								connections = new List<TcpConnection>();
							connections.Add(c);
							ThreadPool.QueueUserWorkItem(c.Work, null);
						} else {
							//TODO: ERROR log ...
						}

					} catch (SocketException e) {
						//TODO: WARN log ...
					}
				}
			} catch(Exception e) {
				//TODO: ERROR log ...
			}
		}

		protected override void OnStart() {
			base.OnStart();

			TcpServiceAddress tcpAddress = (TcpServiceAddress) Address;
			IPEndPoint endPoint = tcpAddress.ToEndPoint();
			listener = new TcpListener(endPoint);
			listener.Server.ReceiveTimeout = 0;
			
			try {
				// listener.Bind(endPoint);
				int curReceiveBufSize = listener.Server.ReceiveBufferSize;
				if (curReceiveBufSize < 256 * 1024) {
					listener.Server.ReceiveBufferSize = 256 * 1024;
				}
				listener.Start(150);
			} catch (IOException e) {
				//TODO: ERROR log ...
				throw;
			}

			polling = true;

			Thread thread = new Thread(Poll);
			thread.IsBackground = true;
			thread.Start();
		}

		protected override void OnStop() {
			polling = false;

			if (connections != null && connections.Count > 0) {
				for (int i = connections.Count - 1; i >= 0; i--) {
					TcpConnection c = connections[i];
					c.Close();
					connections.RemoveAt(i);
				}
			}

			if (listener != null) {
				listener.Stop();
				listener = null;
			}

			base.OnStop();
		}

		#region TcpConnection

		private class TcpConnection {
			private readonly TcpAdminService service;
			private readonly Socket socket;
			private readonly Random random;
			private bool open;

			public TcpConnection(TcpAdminService service, Socket socket) {
				this.service = service;
				this.socket = socket;
				open = true;
				random = new Random();
			}

			private static ResponseMessage NoServiceError(RequestMessage request) {
				ResponseMessage response = request.CreateResponse();
				response.Arguments.Add(new MessageError(new Exception("The service requested is not being run on the instance")));
				return response;
			}

			public void Close() {
				try {
					if (socket != null)
						socket.Close();
				} catch(Exception) {
					service.Logger.Error("Cannot close the socket.");
				} finally {
					open = false;
				}
			}

			public void Work(object state) {
				try {
					// Get as input and output stream on the sockets,
					NetworkStream socketStream = new NetworkStream(socket, FileAccess.ReadWrite);

					BinaryReader reader = new BinaryReader(new BufferedStream(socketStream, 4000), Encoding.Unicode);
					BinaryWriter writer = new BinaryWriter(new BufferedStream(socketStream, 4000), Encoding.Unicode);

					// Write a random long and see if it gets pinged back from the client,
					long rv = (long)random.NextDouble();
					writer.Write(rv);
					writer.Flush();
					long feedback = reader.ReadInt64();
					if (rv != feedback) {
						// Silently close if the value not returned,
						writer.Close();
						reader.Close();
						return;
					}

					// Read the password string from the stream,
					short sz = reader.ReadInt16();
					StringBuilder sb = new StringBuilder(sz);
					for (int i = 0; i < sz; ++i)
						sb.Append(reader.ReadChar());

					string passwordCode = sb.ToString();

					// If it doesn't match, terminate the thread immediately,
					string password = ((TcpServiceConnector) service.Connector).Password;
					if (!passwordCode.Equals(password))
						return;

					// The main command dispatch loop for this connection,
					while (open) {
						// Read the command destination,
						char destination = reader.ReadChar();
						// Exit thread command,
						if (destination == 'e')
							return;

						// Read the message stream object
						IMessageSerializer serializer = service.MessageSerializer;
						RequestMessage requestMessage = (RequestMessage) serializer.Deserialize(reader.BaseStream, MessageType.Request);

						ResponseMessage responseMessage;

						// For analytics
						DateTime benchmark_start = DateTime.Now;

						// Destined for the administration module,
						if (destination == 'a') {
							responseMessage = service.Processor.Process(requestMessage);
						}
							// For a block service in this machine
						else if (destination == 'b') {
							if (service.Block == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Block.Processor.Process(requestMessage);
							}

						}
							// For a manager service in this machine
						else if (destination == 'm') {
							if (service.Manager == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Manager.Processor.Process(requestMessage);
							}
						}
							// For a root service in this machine
						else if (destination == 'r') {
							if (service.Root == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Root.Processor.Process(requestMessage);
							}
						} else {
							throw new IOException("Unknown destination: " + destination);
						}

						// Update the stats
						DateTime benchmark_end = DateTime.Now;
						TimeSpan time_took = benchmark_end - benchmark_start;
						service.Analytics.AddEvent(benchmark_end, time_took);

						// Write and flush the output message,
						serializer.Serialize(responseMessage, writer.BaseStream);
						writer.Flush();

					} // while (true)

				} catch (IOException e) {
					if (e is EndOfStreamException ||
						(e.InnerException is SocketException && 
						((SocketException)e.InnerException).ErrorCode == (int)SocketError.ConnectionReset)) {
						// Ignore this one also,
					} else {
						//TODO: ERROR log ...
					}
				} catch (SocketException e) {
					if (e.ErrorCode == (int)SocketError.ConnectionReset) {
						// Ignore connection reset messages,
					} else {
						//TODO: ERROR log ...
					}
				} finally {
					// Make sure the socket is closed before we return from the thread,
					try {
						socket.Close();
					} catch (IOException e) {
						//TODO: ERROR log ...
					}
				}
			}
		}

		#endregion
	}
}