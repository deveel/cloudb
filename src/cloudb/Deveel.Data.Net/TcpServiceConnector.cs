using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Client;
using Deveel.Data.Net.Security;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	[MessageSerializer(typeof(BinaryRpcMessageSerializer))]
	public class TcpServiceConnector : ServiceConnector {
		public TcpServiceConnector(IServiceAuthenticator authenticator) {
			Authenticator = authenticator;
		}

		// private Dictionary<TcpServiceAddress, TcpConnection> connections;
		// private readonly string password;

		private Thread backgroundThread;
		private bool purgeThreadStopped;
		
		/*
		public string Password {
			get { return password; }
		}
		*/

		protected override bool OnConnect(IServiceAddress address, ServiceType serviceType) {
			// connections = new Dictionary<TcpServiceAddress, TcpConnection>();

			// This thread kills connections that have timed out.
			backgroundThread = new Thread(PurgeConnections);
			backgroundThread.Name = "TCP::PurgeConnections";
			backgroundThread.IsBackground = true;
			backgroundThread.Start();

			return true;
		}

		private void PurgeConnections() {
			try {
				List<TcpConnection> timeoutList = new List<TcpConnection>();

				while (true) {
					timeoutList.Clear();
					lock (Connections) {
						// We check the connections every 2 minutes,
						Monitor.Wait(Connections, 2 * 60 * 1000);
						DateTime now = DateTime.Now;
						List<TcpServiceAddress> toRemove = new List<TcpServiceAddress>();
						// For each key entry,
						foreach (TcpConnection connection in Connections) {
							// If lock is 0, and past timeout, we can safely remove it.
							// The timeout on a connection is 5 minutes plus the poll artifact
							if (connection.lockCount == 0 &&
								connection.lastLockTimestamp.AddMilliseconds(5 * 60 * 1000) < now) {
								toRemove.Add((TcpServiceAddress) connection.Address);
								timeoutList.Add(connection);

							}
						}

						if (toRemove.Count > 0) {
							foreach (TcpServiceAddress address in toRemove)
								Connections.Remove(address);
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
							Logger.Error("Failed to dispose timed out connection", e);
						}
					}

				}
			} catch (ThreadInterruptedException) {
				// Thread was killed,
			}
		}

		private TcpConnection GetConnection(TcpServiceAddress address) {
			IConnection c;
			lock (Connections) {
				// If there isn't, establish a connection,
				if (!Connections.TryGetConnection(address, out c)) {
					IPEndPoint endPoint = address.ToEndPoint();
					Socket socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
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
					c = new TcpConnection(address, socket, Authenticator != null);
					c.Open();
					Connections.Add(c);
				} else {
					((TcpConnection) c).AddLock();
				}
			}
			return c as TcpConnection;
		}

		private void InvalidateConnection(TcpServiceAddress address) {
			lock (Connections) {
				Connections.Remove(address);
			}
		}

		private void ReleaseConnection(TcpConnection c) {
			lock (Connections) {
				c.RemoveLock();
			}
		}

		#region Implementation of IServiceConnector

		public override void Close() {
			lock (Connections) {
				purgeThreadStopped = true;
				Monitor.PulseAll(Connections);
			}
		}

		public IMessageProcessor Connect(TcpServiceAddress address, ServiceType type) {
			return new TcpMessageProcessor(this, address, type);
		}
		
		protected override IMessageProcessor Connect(IServiceAddress address, ServiceType type) {
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

			private Message DoProcess(Message messageStream, int tryCount) {
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

						Message response = serializer.Deserialize(c.Stream, MessageType.Response);
						if (response is MessageStream) {
							return response;
						} else {
							return new ResponseMessage((RequestMessage) messageStream, (ResponseMessage) response);
						}
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

					MessageError error;
					if (e is EndOfStreamException) {
						error = new MessageError(new Exception("EOF (is net password correct?)", e));
					} else {
						// Report this error as a msg_stream fault,
						error = new MessageError(new Exception(e.Message, e));
					}

					Message responseMessage;
					if (messageStream is MessageStream) {
						responseMessage = new MessageStream(MessageType.Response);
						ResponseMessage inner = new ResponseMessage();
						inner.Arguments.Add(error);
						((MessageStream)responseMessage).AddMessage(inner);
					} else {
						responseMessage = ((RequestMessage) messageStream).CreateResponse();
						responseMessage.Arguments.Add(error);
					}

					return responseMessage;
				} finally {
					if (c != null)
						connector.ReleaseConnection(c);
				}
			}

			#region Implementation of IMessageProcessor

			public Message Process(Message messageStream) {
				return DoProcess(messageStream, 0);
			}

			#endregion
		}

		#endregion

		#region TcpConnection

		private sealed class TcpConnection : IConnection {
			internal readonly Socket s;

			private readonly IServiceAddress address;
			private bool opened;
			private bool authenticated;
			private readonly bool authEnabled;
			private Stream stream;

			internal long lockCount;
			internal DateTime lastLockTimestamp;

			public TcpConnection(IServiceAddress address, Socket s, bool authEnabled) {
				this.address = address;
				this.s = s;
				this.authEnabled = authEnabled;
				lockCount = 1;
				lastLockTimestamp = DateTime.Now;
			}

			public Stream Stream {
				get { return stream; }
			}

			/*
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
			*/

			public IServiceAddress Address {
				get { return address; }
			}

			public bool IsOpened {
				get { return opened; }
			}

			public bool IsAuthenticated {
				get { return authenticated; }
			}

			public void Open() {
				stream = new BufferedStream(new NetworkStream(s, FileAccess.ReadWrite), 4000);

				BinaryReader reader = new BinaryReader(stream, Encoding.Unicode);
				BinaryWriter writer = new BinaryWriter(stream, Encoding.Unicode);

				// read the random ID number sent from the service...
				long rv = reader.ReadInt64();

				// ... and resend it to handshake
				writer.Write(rv);

				if (authEnabled) {
					writer.Write((byte)1);
				} else {
					writer.Write((byte)0);
				}

				opened = true;
			}

			public bool EndAuthenticatedSession(object context) {
				return true;
			}

			public void Close() {
				try {
					s.Close();
				} finally {
					opened = false;
				}
			}

			public AuthResponse Authenticate(AuthRequest request) {
				BinaryReader reader = new BinaryReader(stream, Encoding.Unicode);
				BinaryWriter writer = new BinaryWriter(stream, Encoding.Unicode);

				if (authenticated)
					return request.Respond(AuthenticationCode.AlreadyAuthenticated);

				// Send the auth mechanism name
				int mchsz = request.Mechanism.Length;
				writer.Write(mchsz);
				for (int i = 0; i < mchsz; i++) {
					writer.Write(request.Mechanism[i]);
				}

				byte supported = reader.ReadByte();

				if (supported == 0) {
					authenticated = false;
					return request.Respond(AuthenticationCode.UnknownMechanism);
				}

				int c = request.Arguments.Count;
				writer.Write(c);

				foreach (AuthMessageArgument argument in request.Arguments) {
					SendAuthPair(writer, argument.Name, argument.Value);
				}

				// all data sent
				writer.Write(8);

				// read the response
				int code = reader.ReadInt32();

				AuthResponse result = request.Respond(code);

				int outsz = reader.ReadInt32();
				for (int i = 0; i < outsz; i++) {
					GetAuthPair(reader, result);
				}

				return result;
			}

			private static AuthObject GetAuthValue(BinaryReader reader, AuthDataType dataType) {
				if (dataType == AuthDataType.Null)
					return new AuthObject(AuthDataType.Null, null);

				if (dataType == AuthDataType.Binary) {
					int sz = reader.ReadInt32();
					byte[] buffer = new byte[sz];
					reader.Read(buffer, 0, sz);
					return new AuthObject(AuthDataType.Binary, buffer);
				}

				if (dataType == AuthDataType.Boolean) {
					byte b = reader.ReadByte();
					return new AuthObject(AuthDataType.Boolean, b != 0);
				}

				if (dataType == AuthDataType.DateTime) {
					long value = reader.ReadInt64();
					return new AuthObject(AuthDataType.DateTime, DateTime.FromBinary(value));
				}

				if (dataType == AuthDataType.Number) {
					int sz = reader.ReadInt32();
					byte[] data = new byte[sz];
					reader.Read(data, 0, sz);
					double number = BitConverter.ToDouble(data, 0);
					return new AuthObject(AuthDataType.Number, number);
				}

				if (dataType == AuthDataType.String) {
					int sz = reader.ReadInt32();
					StringBuilder sb = new StringBuilder(sz);
					for (int i = 0; i < sz; i++) {
						sb.Append(reader.ReadChar());
					}

					return new AuthObject(AuthDataType.String, sb.ToString());
				}

				if (dataType == AuthDataType.List) {
					AuthDataType listType = (AuthDataType) reader.ReadByte();
					int sz = reader.ReadInt32();
					AuthObject list = new AuthObject(AuthDataType.List);
					for (int i = 0; i < sz; i++) {
						list.Add(GetAuthValue(reader, listType));
					}
					return list;
				}

				throw new InvalidOperationException();
			}

			private static AuthObject GetAuthValue(BinaryReader reader) {
				AuthDataType dataType = (AuthDataType) reader.ReadByte();
				return GetAuthValue(reader, dataType);
			}

			private static void GetAuthPair(BinaryReader reader, AuthResponse result) {
				int keysz = reader.ReadInt32();
				StringBuilder keysb = new StringBuilder(keysz);
				for (int i = 0; i < keysz; i++) {
					keysb.Append(reader.ReadChar());
				}

				string key = keysb.ToString();
				AuthObject value = GetAuthValue(reader);

				result.Arguments.Add(key, value);
			}

			private static void SendAuthValue(BinaryWriter writer, AuthObject value) {
				if (value.IsList) {
					writer.Write((byte) value.ElementType);
					int sz = value.Count;

					writer.Write(sz);
					for (int i = 0; i < sz; i++) {
						SendAuthValue(writer, value[i]);
					}
				} else if (value.DataType == AuthDataType.Binary) {
					byte[] binary = (byte[])value.Value;
					writer.Write(binary.Length);
					writer.Write(binary);
				} else if (value.DataType == AuthDataType.Boolean) {
					bool b = (bool)value.Value;
					writer.Write(b ? 1 : 0);
				} else if (value.DataType == AuthDataType.Null) {
					// null is empty
				} else if (value.DataType == AuthDataType.String) {
					string s = (string)value.Value;
					int sz = s.Length;
					for (int i = 0; i < sz; i++) {
						writer.Write(s[i]);
					}
				} else if (value.DataType == AuthDataType.Number) {
					double number = (double)value.Value;
					byte[] data = BitConverter.GetBytes(number);
					writer.Write(data.Length);
					writer.Write(data);
				}
			}

			private static void SendAuthPair(BinaryWriter writer, string key, AuthObject value) {
				int keysz = key.Length;
				for (int i = 0; i < keysz; i++) {
					writer.Write(key[i]);
				}

				writer.Write((byte)value.DataType);

				SendAuthValue(writer, value);
			}

			public void AddLock() {
				++lockCount;
				lastLockTimestamp = DateTime.Now;
			}

			public void RemoveLock() {
				--lockCount;
			}

			public void Dispose() {
			}
		}

		#endregion
	}
}