using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public class TcpAdminService : AdminService {
		private bool polling;
		private TcpListener listener;
		private List<TcpConnection> connections;

		public TcpAdminService(IServiceFactory serviceFactory, IPAddress address, int port, string password)
			: this(serviceFactory, new TcpServiceAddress(address, port),  password) {
		}
		
		public TcpAdminService(IServiceFactory serviceFactory, IPAddress address, string password)
			: this(serviceFactory, address, TcpServiceAddress.DefaultPort, password) {
		}
		
		public TcpAdminService(IServiceFactory serviceFactory, TcpServiceAddress address, string password)
			: base(address, new TcpServiceConnector(password), serviceFactory) {
		}
		
		public bool IsListening {
			get { return polling; }
		}

		private void Poll() {
			try {				
				Logger.Info("Node started on " + Address);

				while (polling) {
					if (!listener.Pending()) {
						Thread.Sleep(500);
						continue;
					}

					Socket s;
					try {
						// The socket to run the service,
						s = listener.AcceptSocket();
						s.NoDelay = true;
						int curSendBufSize = s.SendBufferSize;
						if (curSendBufSize < 256 * 1024) {
							s.SendBufferSize = 256 * 1024;
						}

						// Make sure this ip address is allowed,
						IPAddress ipAddress = ((IPEndPoint)s.RemoteEndPoint).Address;

						Logger.Info("Connection from " + ipAddress);

						bool authorized = IPAddress.IsLoopback(ipAddress) ||
						                  IsAddressAllowed(ipAddress.ToString());

						if (OnClientConnect(ipAddress.ToString(), authorized)) {
							// Dispatch the connection to the thread pool,
							TcpConnection c = new TcpConnection(this, s);
							if (connections == null)
								connections = new List<TcpConnection>();
							connections.Add(c);
							ThreadPool.QueueUserWorkItem(c.Work, null);
						} else {
							Logger.Error("Connection refused from " + ipAddress + ": not allowed");
						}

					} catch (SocketException e) {
						Logger.Warning("Socket Errot while processing a connection.", e);
					}
				}
			} catch(Exception e) {
				Logger.Error("Error while polling.", e);
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
			} catch (SocketException e) {
				Logger.Error("Socket Error while starting the TCP Admin service.", e);
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
			private readonly string remoteEndPoint;
			private bool open;

			public TcpConnection(TcpAdminService service, Socket socket) {
				this.service = service;
				this.socket = socket;

				remoteEndPoint = socket.RemoteEndPoint.ToString();
				open = true;
				random = new Random();
			}

			private static IEnumerable<Message> NoServiceError() {
				return new Message(new MessageError(new Exception("The service requested is not being run on the instance"))).AsStream();
			}

			public void Close() {
				try {
					if (socket != null && socket.Connected)
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

					/*
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
					*/

					if (!service.Connector.Authenticator.Authenticate(AuthenticationPoint.Service, socketStream))
						return;

					// The main command dispatch loop for this connection,
					while (open) {
						// Read the command destination,
						char destination = reader.ReadChar();
						// Exit thread command,
						if (destination == 'e') {
							service.OnClientDisconnect(remoteEndPoint);
							return;
						}

						// Read the message stream object
						IMessageSerializer serializer = service.MessageSerializer;
						IEnumerable<Message> requestMessage = serializer.Deserialize(reader.BaseStream);

						service.OnClientRequest(service.ServiceType, remoteEndPoint, requestMessage);

						IEnumerable<Message> responseMessage;

						// For analytics
						DateTime benchmarkStart = DateTime.Now;

						// Destined for the administration module,
						if (destination == 'a') {
							responseMessage = service.Processor.Process(requestMessage);
						}
							// For a block service in this machine
						else if (destination == 'b') {
							if (service.Block == null) {
								responseMessage = NoServiceError();
							} else {
								responseMessage = service.Block.Processor.Process(requestMessage);
							}

						}
							// For a manager service in this machine
						else if (destination == 'm') {
							if (service.Manager == null) {
								responseMessage = NoServiceError();
							} else {
								responseMessage = service.Manager.Processor.Process(requestMessage);
							}
						}
							// For a root service in this machine
						else if (destination == 'r') {
							if (service.Root == null) {
								responseMessage = NoServiceError();
							} else {
								responseMessage = service.Root.Processor.Process(requestMessage);
							}
						} else {
							throw new IOException("Unknown destination: " + destination);
						}

						service.OnClientResponse(remoteEndPoint, responseMessage);

						// Update the stats
						DateTime benchmarkEnd = DateTime.Now;
						TimeSpan timeTook = benchmarkEnd - benchmarkStart;
						service.Analytics.AddEvent(benchmarkEnd, timeTook);

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
						service.Logger.Error("IO Error during connection input", e);
					}
				} catch (SocketException e) {
					if (e.ErrorCode == (int)SocketError.ConnectionReset) {
						// Ignore connection reset messages,
					} else {
						service.Logger.Error("Socket Error during connection input", e);
					}
				} finally {
					// Make sure the socket is closed before we return from the thread,
					try {
						socket.Close();
					} catch (Exception e) {
						service.Logger.Error("Error on connection close", e);
					}
				}
			}
		}

		#endregion
	}
}