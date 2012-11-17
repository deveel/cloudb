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
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public sealed class TcpServiceConnector : ServiceConnector {
		private readonly Dictionary<IServiceAddress, TcpConnection> connectionPool;
		private readonly ConnectionDestroyThread connectionDestroy;

		private int introducedLatency;

		public TcpServiceConnector(IServiceAuthenticator authenticator) {
			Authenticator = authenticator;
			connectionPool = new Dictionary<IServiceAddress, TcpConnection>();

			connectionDestroy = new ConnectionDestroyThread(this);
		}

		public TcpServiceConnector()
			: this(NoAuthenticationAuthenticator.Instance) {
		}

		public TcpServiceConnector(string password)
			: this(new PasswordAuthenticator(password)) {
		}

		public int IntroducedLatency {
			get { return introducedLatency; }
			set { introducedLatency = value; }
		}

		protected override IMessageProcessor Connect(IServiceAddress address, ServiceType type) {
			return new MessageProcessor(this, (TcpServiceAddress) address, type);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				connectionDestroy.Stop();
			}

			base.Dispose(disposing);
		}

		private TcpConnection GetConnection(TcpServiceAddress address) {
			TcpConnection c;
			lock (connectionPool) {
				// If there isn't, establish a connection,
				if (!connectionPool.TryGetValue(address, out c)) {
					Socket socket = new Socket(address.IsIPv4 ? AddressFamily.InterNetwork : AddressFamily.InterNetworkV6,
					                           SocketType.Stream, ProtocolType.IP);
					socket.Connect(address.ToIPAddress(), address.Port);
					socket.ReceiveTimeout = (30*1000); // 30 second timeout,
					socket.NoDelay = true;
					int curSendBufSize = socket.SendBufferSize;
					if (curSendBufSize < 256*1024)
						socket.SendBufferSize = 256*1024;

					int curReceiveBufSize = socket.ReceiveBufferSize;
					if (curReceiveBufSize < 256*1024)
						socket.ReceiveBufferSize = 256*1024;

					c = new TcpConnection(this, socket);
					c.Connect();
					connectionPool[address] = c;
				} else {
					c.AddLock();
				}
			}
			return c;
		}

		private void InvalidateConnection(TcpServiceAddress address) {
			lock (connectionPool) {
				connectionPool.Remove(address);
			}
		}

		private void ReleaseConnection(TcpConnection c) {
			lock (connectionPool) {
				c.RemoveLock();
			}
		}


		#region TcpConnection

		private class TcpConnection {
			private readonly TcpServiceConnector connector;
			private readonly Socket socket;
			private Stream stream;

			private long lockCount;
			private DateTime lastLockTimestamp;

			public TcpConnection(TcpServiceConnector connector, Socket socket) {
				this.connector = connector;
				this.socket = socket;
				lockCount = 1;
				lastLockTimestamp = DateTime.Now;
			}

			public Socket Socket {
				get { return socket; }
			}

			public Stream Stream {
				get { return stream; }
			}

			public long LockCount {
				get { return lockCount; }
			}

			public DateTime LastLockTimestamp {
				get { return lastLockTimestamp; }
			}

			public void Connect() {
				stream = new BufferedStream(new NetworkStream(socket, FileAccess.ReadWrite), 4000);

				/*
				BinaryReader din = new BinaryReader(stream, Encoding.Unicode);
				long rv = din.ReadInt64();

				// Send the password,
				BinaryWriter dout = new BinaryWriter(stream, Encoding.Unicode);
				dout.Write(rv);
				short sz = (short) password.Length;
				dout.Write(sz);
				for (int i = 0; i < sz; ++i) {
					dout.Write(password[i]);
				}
				dout.Flush();
				*/

				//TODO: report an eventual failed uthentication?
				connector.Authenticator.Authenticate(AuthenticationPoint.Client, stream);
			}

			public void Close() {
				socket.Close();
			}

			public void AddLock() {
				++lockCount;
				lastLockTimestamp = DateTime.Now;
			}

			public void RemoveLock() {
				--lockCount;
			}
		}

		#endregion

		#region ConnectionDestroyThread

		public class ConnectionDestroyThread {
			private readonly TcpServiceConnector connector;
			private readonly Thread thread;
			private bool stopped = false;

			public ConnectionDestroyThread(TcpServiceConnector connector) {
				this.connector = connector;
				thread = new Thread(Execute);
				thread.Name = "TcpServceConnector::ConnectionDestroy";
				thread.IsBackground = true;
				thread.Start();
			}

			private void Execute() {
				try {
					List<TcpConnection> timeoutList = new List<TcpConnection>();
					while (true) {
						timeoutList.Clear();
						lock (connector.connectionPool) {
							// We check the connections every 2 minutes,
							Monitor.Wait(connector.connectionPool, 2*60*1000);
							DateTime timeNow = DateTime.Now;

							IEnumerable<IServiceAddress> s = new List<IServiceAddress>(connector.connectionPool.Keys);
							// For each key entry,
							foreach (IServiceAddress address in s) {
								TcpConnection c = connector.connectionPool[address];
								// If lock is 0, and past timeout, we can safely remove it.
								// The timeout on a connection is 5 minutes plus the poll artifact
								if (c.LockCount == 0 &&
								    c.LastLockTimestamp.AddMilliseconds((5*60*1000)) < timeNow) {

									connector.connectionPool.Remove(address);
									timeoutList.Add(c);

								}
							}

							// If the thread was stopped, we finish the run method which stops
							// the thread.
							if (stopped) {
								return;
							}

						} // synchronized (connection_pool)

						// For each connection that timed out,
						foreach (TcpConnection c in timeoutList) {
							BinaryWriter dout = new BinaryWriter(c.Stream, Encoding.Unicode);
							// Write the stream close message, and flush,
							try {
								dout.Write('e');
								dout.Flush();
								c.Socket.Close();
							} catch (IOException e) {
								connector.Logger.Error("Failed to dispose timed out connection", e);
							}
						}

					}
				} catch (ThreadInterruptedException e) {
					// Thread was killed,
				}

			}

			public void Stop() {
				lock (connector.connectionPool) {
					stopped = true;
					Monitor.PulseAll(connector.connectionPool);
				}
			}
		}

		#endregion

		#region MessageProcessor

		class MessageProcessor : IMessageProcessor {
			private readonly TcpServiceConnector connector;
			private readonly TcpServiceAddress address;
			private readonly ServiceType serviceType;

			public MessageProcessor(TcpServiceConnector connector, TcpServiceAddress address, ServiceType serviceType) {
				this.connector = connector;
				this.address = address;
				this.serviceType = serviceType;
			}

			private IEnumerable<Message> ProcessInternal(IEnumerable<Message> msgStream, int tryCount) {
				TcpConnection c = null;
				try {
					// Check if there's a connection in the pool already,
					c = connector.GetConnection(address);

					lock (c) {
						BinaryWriter dout = new BinaryWriter(c.Stream);

						// Write the message.
						dout.Write((byte)serviceType);
						connector.MessageSerializer.Serialize(msgStream, c.Stream);
						dout.Flush();

						// Fetch the result,

						IEnumerable<Message> msgResult = connector.MessageSerializer.Deserialize(c.Stream);

						// If there's a test latency,
						if (connector.IntroducedLatency > 0) {
							try {
								Thread.Sleep(connector.IntroducedLatency);
							} catch (ThreadInterruptedException) {
								// Ignore
							}
						}

						// And return it,
						return msgResult;
					}

				} catch (Exception e) {
					// If this is a 'connection reset by peer' error, wipe the connection
					// from the cache and retry connection,
					if (tryCount == 0 &&
					    (e is SocketException ||
					     e is EndOfStreamException)) {
						connector.InvalidateConnection(address);
						// And retry,
						return ProcessInternal(msgStream, tryCount + 1);
					}

					Message msgResult;
					if (e is EndOfStreamException) {
						msgResult = new Message(new MessageError(new ApplicationException("EOF (is net password correct?)", e)));
					} else {
						// Report this error as a msg_stream fault,
						msgResult = new Message(new MessageError(e));
					}

					return msgResult.AsStream();
				}
					// Make sure we release the connection,
				finally {
					if (c != null) {
						connector.ReleaseConnection(c);
					}
				}
			}


			public IEnumerable<Message> Process(IEnumerable<Message> stream) {
				return ProcessInternal(stream, 0);
			}
		}

		#endregion
	}
}