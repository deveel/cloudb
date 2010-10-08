using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class TcpServiceConnector : IServiceConnector {
		public TcpServiceConnector(string password) {
			connections = new Dictionary<TcpServiceAddress, TcpConnection>();
			this.password = password;

			// This thread kills connections that have timed out.
			backgroundThread = new Thread(PurgeConnections);
			backgroundThread.Name = "TCP::PurgeConnections";
			backgroundThread.IsBackground = true;
			backgroundThread.Start();
		}

		private readonly Dictionary<TcpServiceAddress, TcpConnection> connections;
		private readonly string password;

		private readonly Thread backgroundThread;
		private bool purgeThreadStopped;

		public string Password {
			get { return password; }
		}

		private void PurgeConnections() {
			try {
				List<TcpConnection> timeoutList = new List<TcpConnection>();

				while (true) {
					timeoutList.Clear();
					lock (connections) {
						// We check the connections every 2 minutes,
						Monitor.Wait(connections, 2 * 60 * 1000);
						DateTime now = DateTime.Now;
						List<TcpServiceAddress> toRemove = new List<TcpServiceAddress>();
						// For each key entry,
						foreach (KeyValuePair<TcpServiceAddress, TcpConnection> connection in connections) {
							// If lock is 0, and past timeout, we can safely remove it.
							// The timeout on a connection is 5 minutes plus the poll artifact
							if (connection.Value.lock_count == 0 &&
								connection.Value.last_lock_timestamp.AddMilliseconds(5 * 60 * 1000) < now) {
								toRemove.Add(connection.Key);
								timeoutList.Add(connection.Value);

							}
						}

						if (toRemove.Count > 0) {
							foreach (TcpServiceAddress address in toRemove)
								connections.Remove(address);
						}

						// If the thread was stopped, we finish the run method which stops
						// the thread.
						if (purgeThreadStopped)
							return;
					}

					// For each connection that timed out,
					foreach (TcpConnection c in timeoutList) {
						BinaryWriter dout = new BinaryWriter(c.Stream, Encoding.Unicode);
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

		private TcpConnection GetConnection(TcpServiceAddress address) {
			TcpConnection c;
			lock (connections) {
				// If there isn't, establish a connection,
				if (!connections.TryGetValue(address, out c)) {
					IPEndPoint endPoint = address.ToEndPoint();
					Socket socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.IP);
#if DEBUG
					socket.ReceiveTimeout = Int32.MaxValue;
#else
					socket.ReceiveTimeout = 8 * 1000;  // 8 second timeout,
#endif
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

		private void InvalidateConnection(TcpServiceAddress address) {
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

		public IMessageProcessor Connect(TcpServiceAddress address, ServiceType type) {
			return new TcpMessageProcessor(this, address, type);
		}
		
		IMessageProcessor IServiceConnector.Connect(IServiceAddress address, ServiceType type) {
			return Connect((TcpServiceAddress)address, type);
		}

		#endregion

		#region TcpMessageProcessor

		private class TcpMessageProcessor : IMessageProcessor {
			public TcpMessageProcessor(TcpServiceConnector connector, TcpServiceAddress address, ServiceType serviceType) {
				this.connector = connector;
				this.address = address;
				this.serviceType = serviceType;
			}

			private readonly TcpServiceAddress address;
			private readonly ServiceType serviceType;
			private readonly TcpServiceConnector connector;

			private ResponseMessage DoProcess(RequestMessage messageStream, int tryCount) {
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

						BinaryWriter writer = new BinaryWriter(c.Stream, Encoding.Unicode);
						writer.Write(code);

						IMessageSerializer serializer = new BinaryRpcMessageSerializer();
						serializer.Serialize(messageStream, c.Stream);
						writer.Flush();

						ResponseMessage response = (ResponseMessage) serializer.Deserialize(c.Stream, MessageType.Response);
						return new ResponseMessage(messageStream, response);
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

					ResponseMessage responseMessage;
					if (messageStream is RequestMessageStream) {
						responseMessage = new ResponseMessageStream();
						ResponseMessage inner = new ResponseMessage();
						inner.Arguments.Add(error);
						((ResponseMessageStream)responseMessage).AddMessage(inner);
					} else {
						responseMessage = messageStream.CreateResponse();
					}

					return responseMessage;
				} finally {
					if (c != null)
						connector.ReleaseConnection(c);
				}
			}

			#region Implementation of IMessageProcessor

			public ResponseMessage Process(RequestMessage messageStream) {
				return DoProcess(messageStream, 0);
			}

			#endregion
		}

		#endregion

		#region TcpConnection

		private sealed class TcpConnection {
			internal readonly Socket s;

			private Stream stream;
			internal long lock_count;

			internal DateTime last_lock_timestamp;

			public TcpConnection(Socket s) {
				this.s = s;
				lock_count = 1;
				last_lock_timestamp = DateTime.Now;
			}

			public Stream Stream {
				get { return stream; }
			}

			public void Connect(String password) {
				stream = new BufferedStream(new NetworkStream(s, FileAccess.ReadWrite), 4000);

				BinaryReader din = new BinaryReader(stream, Encoding.Unicode);
				BinaryWriter dout = new BinaryWriter(stream, Encoding.Unicode);
				long rv = din.ReadInt64();

				// Send the password,
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