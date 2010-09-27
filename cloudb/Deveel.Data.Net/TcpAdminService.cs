using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net {
	public class TcpAdminService : AdminService {
		private NetworkConfigSource config;
		private bool polling;
		private TcpListener listener;

		public TcpAdminService(IAdminServiceDelegator delegator, IPAddress address, int port, string password)
			: this(delegator, address, password) {
		}
		
		public TcpAdminService(IAdminServiceDelegator delegator, IPAddress address, string password)
			: this(delegator, address, TcpServiceAddress.DefaultPort, password) {
		}
		
		public TcpAdminService(IAdminServiceDelegator delegator, TcpServiceAddress address, string password)
			: base(address, new TcpServiceConnector(password), delegator) {
		}
		
		public NetworkConfigSource Config {
			get { return config; }
			set { config = value; }
		}

		private void ConfigUpdate(object state) {
			NetworkConfigSource source = (NetworkConfigSource)state;
			source.Reload();
		}

		private void Poll() {
			Timer timer = new Timer(ConfigUpdate);

			try {
				// Schedule a refresh of the config file,
				// (We add a little entropy to ensure the network doesn't get hit by
				//  synchronized requests).
				Random r = new Random();
				long second_mix = r.Next(1000);
				timer.Change(50 * 1000, ((2 * 59) * 1000) + second_mix);
				
				//TODO: INFO log ...

				while (polling) {
					TcpClient s;
					try {
						// The socket to run the service,
						s = listener.AcceptTcpClient();
						s.NoDelay = true;
						int cur_send_buf_size = s.SendBufferSize;
						if (cur_send_buf_size < 256 * 1024) {
							s.SendBufferSize = 256 * 1024;
						}

						// Make sure this ip address is allowed,
						IPAddress ipAddress = ((IPEndPoint)s.Client.RemoteEndPoint).Address;

						//TODO: INFO log ...

						// Check it's allowed,
						if (ipAddress.IsIPv6LinkLocal ||
							IPAddress.IsLoopback(ipAddress) ||
							config.IsIpAllowed(ipAddress.ToString())) {
							// Dispatch the connection to the thread pool,
							TcpConnection c = new TcpConnection(this);
							ThreadPool.QueueUserWorkItem(c.Work, s);
						} else {
							//TODO: ERROR log ...
						}

					} catch (IOException e) {
						//TODO: WARN log ...
					}
				}
			} finally {
				timer.Dispose();
			}
		}

		protected override void OnInit() {
			TcpServiceAddress tcpAddress = (TcpServiceAddress) Address;
			IPEndPoint endPoint = tcpAddress.ToEndPoint();
			listener = new TcpListener(endPoint);
			listener.Server.ReceiveTimeout = 0;
			
			try {
				listener.Start(150);
				int curReceiveBufSize = listener.Server.ReceiveBufferSize;
				if (curReceiveBufSize < 256 * 1024) {
					listener.Server.ReceiveBufferSize = 256 * 1024;
				}
			} catch (IOException e) {
				//TODO: ERROR log ...
				throw;
			}


			Thread thread = new Thread(Poll);
			thread.IsBackground = true;
			thread.Start();
		}

		protected override void OnDispose(bool disposing) {
			if (disposing) {
				polling = false;
				if (listener != null) {
					listener.Stop();
					listener = null;
				}
			}
		}

		#region TcpConnection

		private class TcpConnection {
			private readonly TcpAdminService service;
			private readonly Random random;

			public TcpConnection(TcpAdminService service) {
				this.service = service;
				random = new Random();
			}

			private static MessageStream NoServiceError() {
				MessageStream msg_out = new MessageStream(16);
				msg_out.AddErrorMessage(new ServiceException(new Exception("The service requested is not being run on the instance")));
				return msg_out;
			}

			public void Work(object state) {
				TcpClient s = (TcpClient)state;
				try {
					// Get as input and output stream on the sockets,
					NetworkStream socketStream = s.GetStream();

					BinaryReader reader = new BinaryReader(new BufferedStream(socketStream, 4000), Encoding.UTF8);
					BinaryWriter writer = new BinaryWriter(new BufferedStream(socketStream, 4000), Encoding.UTF8);

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
					while (true) {
						// Read the command destination,
						char destination = reader.ReadChar();
						// Exit thread command,
						if (destination == 'e')
							return;

						// Read the message stream object
						BinaryMessageStreamSerializer serializer = new BinaryMessageStreamSerializer();
						MessageStream message_stream = serializer.Deserialize(reader);

						MessageStream message_out;

						// For analytics
						DateTime benchmark_start = DateTime.Now;

						// Destined for the administration module,
						if (destination == 'a') {
							message_out = service.Processor.Process(message_stream);
						}
							// For a block service in this machine
						else if (destination == 'b') {
							if (service.Block == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Block.Processor.Process(message_stream);
							}

						}
							// For a manager service in this machine
						else if (destination == 'm') {
							if (service.Manager == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Manager.Processor.Process(message_stream);
							}
						}
							// For a root service in this machine
						else if (destination == 'r') {
							if (service.Root == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Root.Processor.Process(message_stream);
							}
						} else {
							throw new IOException("Unknown destination: " + destination);
						}

						// Update the stats
						DateTime benchmark_end = DateTime.Now;
						TimeSpan time_took = benchmark_end - benchmark_start;
						service.Analytics.AddEvent(benchmark_end, time_took);

						// Write and flush the output message,
						serializer.Serialize(message_out, writer);
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
						s.Close();
					} catch (IOException e) {
						//TODO: ERROR log ...
					}
				}
			}
		}

		#endregion
	}
}