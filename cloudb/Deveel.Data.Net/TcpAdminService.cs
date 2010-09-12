using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net {
	public abstract class TcpAdminService : AdminService {
		private readonly string password;
		private readonly NetworkConfigSource config;
		private readonly ServiceAddress serviceAddress;
		private bool polling;

		protected TcpAdminService(NetworkConfigSource config, IPAddress address, int port, string  password) {
			this.config = config;
			serviceAddress = new ServiceAddress(address, port);
			this.password = password;
		}

		protected string Password {
			get { return password; }
		}

		protected ServiceAddress Address {
			get { return serviceAddress; }
		}

		private void ConfigLog() {
			//TODO:
		}

		private void ConfigUpdate(object state) {
			//TODO:
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

				Socket socket;
				try {
					IPEndPoint endPoint = serviceAddress.ToEndPoint();
					socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
					socket.Bind(endPoint);
					socket.ReceiveTimeout = 0;
					socket.Listen(150);
					int cur_receive_buf_size = socket.ReceiveBufferSize;
					if (cur_receive_buf_size < 256 * 1024) {
						socket.ReceiveBufferSize = 256 * 1024;
					}
				} catch (IOException e) {
					//TODO: ERROR log ...
					return;
				}

				//TODO: INFO log ...

				while (polling) {
					Socket s;
					try {
						// The socket to run the service,
						s = socket.Accept();
						s.NoDelay = true;
						int cur_send_buf_size = s.SendBufferSize;
						if (cur_send_buf_size < 256 * 1024) {
							s.SendBufferSize = 256 * 1024;
						}

						// Make sure this ip address is allowed,
						IPAddress ipAddress = ((IPEndPoint)s.LocalEndPoint).Address;

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
			ConfigLog();

			Thread thread = new Thread(Poll);
			thread.IsBackground = true;
			thread.Start();
		}

		protected override void OnDispose(bool disposing) {
			if (disposing)
				polling = false;
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
				Socket s = (Socket)state;
				try {
					// Get as input and output stream on the sockets,
					NetworkStream socketStream = new NetworkStream(s, FileAccess.ReadWrite);

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
					if (!passwordCode.Equals(service.password))
						return;

					// The main command dispatch loop for this connection,
					while (true) {
						// Read the command destination,
						char destination = reader.ReadChar();
						// Exit thread command,
						if (destination == 'e')
							return;

						// Read the message stream object
						MessageStreamSerializer serializer = new MessageStreamSerializer();
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