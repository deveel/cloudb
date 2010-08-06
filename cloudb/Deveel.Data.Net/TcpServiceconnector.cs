using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net {
	public class TcpServiceConnector : IServiceConnector {
		public TcpServiceConnector(string password) {
			connections = new Dictionary<ServiceAddress, TcpConnection>();
			this.password = password;

			// This thread kills connections that have timed out.
			backgroundThread = new Thread(PurgeConnections);
			backgroundThread.Name = "TCP::PurgeConnections";
			backgroundThread.IsBackground = true;
			backgroundThread.Start();
		}

		private readonly Dictionary<ServiceAddress, TcpConnection> connections;
		private readonly string password;

		private readonly Thread backgroundThread;
		private bool purgeThreadStopped;

		private void PurgeConnections() {
			try {
				List<TcpConnection> timeoutList = new List<TcpConnection>();

				while (true) {
					timeoutList.Clear();
					lock (connections) {
						// We check the connections every 2 minutes,
						Monitor.Wait(connections, 2 * 60 * 1000);
						DateTime now = DateTime.Now;
						List<ServiceAddress> toRemove = new List<ServiceAddress>();
						// For each key entry,
						foreach (KeyValuePair<ServiceAddress, TcpConnection> connection in connections) {
							// If lock is 0, and past timeout, we can safely remove it.
							// The timeout on a connection is 5 minutes plus the poll artifact
							if (connection.Value.lock_count == 0 &&
								connection.Value.last_lock_timestamp.AddMilliseconds(5 * 60 * 1000) < now) {
								toRemove.Add(connection.Key);
								timeoutList.Add(connection.Value);

							}
						}

						if (toRemove.Count > 0) {
							foreach (ServiceAddress address in toRemove)
								connections.Remove(address);
						}

						// If the thread was stopped, we finish the run method which stops
						// the thread.
						if (purgeThreadStopped)
							return;
					}

					// For each connection that timed out,
					foreach (TcpConnection c in timeoutList) {
						BinaryWriter dout = new BinaryWriter(c.Output, Encoding.Unicode);
						// Write the stream close message, and flush,
						try {
							dout.Write('e');
							dout.Flush();
							c.s.Close();
						} catch (IOException e) {
							//TODO: ERROR log ...
						}
					}

				}
			} catch (ThreadInterruptedException e) {
				// Thread was killed,
			}
		}

		private TcpConnection GetConnection(ServiceAddress address) {
			TcpConnection c;
			lock (connections) {
				// If there isn't, establish a connection,
				if (!connections.TryGetValue(address, out c)) {
					IPEndPoint endPoint = address.ToEndPoint();
					Socket socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
					socket.ReceiveTimeout = 8 * 1000;  // 8 second timeout,
					socket.NoDelay = true;

					int curSendBufSize = socket.SendBufferSize;
					if (curSendBufSize < 256 * 1024)
						socket.SendBufferSize = 256 * 1024;

					int curReceiveBufSize = socket.ReceiveBufferSize;
					if (curReceiveBufSize < 256 * 1024)
						socket.ReceiveBufferSize = 256 * 1024;

					socket.Connect(endPoint);
					c = new TcpConnection(socket);
					c.Connect(password);
					connections.Add(address, c);
				} else {
					c.AddLock();
				}
			}
			return c;
		}

		private void InvalidateConnection(ServiceAddress address) {
			lock (connections) {
				connections.Remove(address);
			}
		}

		private void ReleaseConnection(TcpConnection c) {
			lock (connections) {
				c.RemoveLock();
			}
		}

		#region Implementation of IDisposable

		public void Dispose() {
			Close();
		}

		#endregion

		#region Implementation of IServiceConnector

		public void Close() {
			lock (connections) {
				purgeThreadStopped = true;
				Monitor.PulseAll(connections);
			}
		}

		public IMessageProcessor Connect(ServiceAddress address, ServiceType type) {
			return new TcpMessageProcessor(this, address, type);
		}

		#endregion

		#region TcpMessageProcessor

		private class TcpMessageProcessor : IMessageProcessor {
			public TcpMessageProcessor(TcpServiceConnector connector, ServiceAddress address, ServiceType serviceType) {
				this.connector = connector;
				this.address = address;
				this.serviceType = serviceType;
			}

			private readonly ServiceAddress address;
			private readonly ServiceType serviceType;
			private readonly TcpServiceConnector connector;

			private MessageStream DoProcess(MessageStream messageStream, int tryCount) {
				TcpConnection c = null;

				try {
					// Check if there's a connection in the pool already,
					c = connector.GetConnection(address);

					lock (c) {
						// Write the message.
						char code = '\0';
						if (serviceType == ServiceType.Manager) {
							code = 'm';
						} else if (serviceType == ServiceType.Root) {
							code = 'r';
						} else if (serviceType == ServiceType.Block) {
							code = 'b';
						} else if (serviceType == ServiceType.Admin) {
							code = 'a';
						}

						BinaryWriter writer = new BinaryWriter(c.Output, Encoding.Unicode);
						writer.Write(code);
						MessageStreamSerializer serializer = new MessageStreamSerializer();
						serializer.Serialize(messageStream, writer);
						writer.Flush();

						BinaryReader reader = new BinaryReader(c.Input, Encoding.Unicode);
						return serializer.Deserialize(reader);
					}
				} catch (Exception e) {
					// If this is a 'connection reset by peer' error, wipe the connection
					// from the cache and retry connection,
					if (tryCount == 0 &&
						(e is SocketException ||
						 e is EndOfStreamException)) {
						connector.InvalidateConnection(address);
						// And retry,
						return DoProcess(messageStream, tryCount + 1);
					}

					ServiceException error;
					if (e is EndOfStreamException) {
						error = new ServiceException(new Exception("EOF (is net password correct?)", e));
					} else {
						// Report this error as a msg_stream fault,
						error = new ServiceException(new Exception(e.Message, e));
					}

					MessageStream outputStream = new MessageStream(16);
					outputStream.AddMessage(new ErrorMessage(error));
					return outputStream;
				} finally {
					if (c != null)
						connector.ReleaseConnection(c);
				}
			}

			#region Implementation of IMessageProcessor

			public MessageStream Process(MessageStream messageStream) {
				return DoProcess(messageStream, 0);
			}

			#endregion
		}

		#endregion

		#region TcpConnection

		private sealed class TcpConnection {
			internal readonly Socket s;

			private Stream input;
			private Stream output;

			internal long lock_count;

			internal DateTime last_lock_timestamp;

			public TcpConnection(Socket s) {
				this.s = s;
				lock_count = 1;
				last_lock_timestamp = DateTime.Now;
			}

			public Stream Input {
				get { return input; }
			}

			public Stream Output {
				get { return output; }
			}

			public void Connect(String password) {
				input = new BufferedStream(new NetworkStream(s, FileAccess.Read), 4000);
				output = new BufferedStream(new NetworkStream(s, FileAccess.Write), 4000);

				BinaryReader din = new BinaryReader(input, Encoding.Unicode);
				long rv = din.ReadInt64();

				// Send the password,
				BinaryWriter dout = new BinaryWriter(output, Encoding.Unicode);
				dout.Write(rv);
				short sz = (short)password.Length;
				dout.Write(sz);
				for (int i = 0; i < sz; ++i) {
					dout.Write(password[i]);
				}
				dout.Flush();
			}

			public void Close() {
				s.Close();
			}

			public void AddLock() {
				++lock_count;
				last_lock_timestamp = DateTime.Now;
			}

			public void RemoveLock() {
				--lock_count;
			}

		}

		#endregion
	}
}