using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	class ReplicatedValueStore {
		private readonly IServiceAddress address;
		private readonly IServiceConnector connector;
		private readonly IDatabase blockDatabase;

		private readonly object blockDbWriteLock;

		private readonly ServiceStatusTracker tracker;

		private readonly MessageCommunicator comm;

		private readonly List<IServiceAddress> cluster;

		private static readonly RNGCryptoServiceProvider Random = new RNGCryptoServiceProvider();

		private volatile long[] lastCompletedUid = null;


		private volatile bool connected;

		private bool initValBoolean;
		private readonly object initValLock = new Object();

		private static readonly Logger Log = Logger.Network;

		private static readonly Key UidListKey = new Key(12, 0, 40);
		private static readonly Key BlockidUidMapKey = new Key(12, 0, 50);
		private static readonly Key KeyUidMapKey = new Key(12, 0, 60);

		internal ReplicatedValueStore(IServiceAddress address,
		                              IServiceConnector connector,
		                              IDatabase localDb, Object dbWriteLock,
		                              ServiceStatusTracker tracker) {

			this.address = address;
			this.connector = connector;
			blockDatabase = localDb;
			blockDbWriteLock = dbWriteLock;
			this.tracker = tracker;
			comm = new MessageCommunicator(connector, tracker);

			cluster = new List<IServiceAddress>(9);
			ClearAllMachines();

			// Add listener for service status updates,
			tracker.StatusChange += TrackerOnStatusChange;

		}

		private void TrackerOnStatusChange(object sender, ServiceStatusEventArgs args) {
			if (args.ServiceType == ServiceType.Manager) {
				// If we detected that a manager is now available,
				if (args.NewStatus == ServiceStatus.Up) {
					// If this is not connected, initialize,
					if (!connected) {
						Initialize();
					} else {
						// Retry any pending messages on this service,
						comm.RetryMessagesFor(address);
					}
				} else if (args.NewStatus != ServiceStatus.Up) {
					// If it's connected,
					if (connected) {
						int clusterSize;
						lock (cluster) {
							clusterSize = cluster.Count;
						}
						// Set connected to false if availability check fails,
						CheckClusterAvailability(clusterSize);
					}
				}
				// If a manager server goes down,
			}
		}


		private readonly Object initLock = new Object();

		private void InitTask(object state) {
			DoInitialize();
		}

		public void Initialize() {
			lock (initValLock) {
				initValBoolean = true;
			}

			new Timer(InitTask, null, 500, Timeout.Infinite);
		}

		private void DoInitialize() {
			try {
				lock (initLock) {
					// If already connected, return
					if (connected)
						return;

					List<IServiceAddress> machines = new List<IServiceAddress>(17);
					lock (cluster) {
						machines.AddRange(cluster);
					}

					List<IServiceAddress> synchronizedOn = new List<IServiceAddress>(17);

					// For each machine in the cluster,
					foreach (IServiceAddress machine in machines) {
						if (!machine.Equals(address)) {

							// Request all messages from the machines log from the given last
							// update time.

							LogEntryIterator messages = RequestMessagesFrom(machine, GetLatestUid());

							if (messages != null) {
								Log.Info(String.Format("Synchronizing with {0}", machine));

								try {
									long synchronizeCount = PlaybackMessages(messages);

									Log.Info(String.Format("{0} synching with {1} complete, message count = {2}", address, machine,
									                       synchronizeCount));

									// Add the machine to the synchronized_on list
									synchronizedOn.Add(machine);
								} catch (ApplicationException e) {
									// If we failed to sync, report the manager down
									Log.Info(String.Format("Failed synchronizing with {0}", machine));
									Log.Info("Sync Fail Error", e);
									tracker.ReportServiceDownClientReport(machine, ServiceType.Manager);
								}
							}
						}
					}

					// If we lock on a majority of the machines, set the connected
					// flag to true,
					if ((synchronizedOn.Count + 1) > machines.Count/2) {
						Log.Info(String.Format("  **** Setting connected to true {0}", address));
						connected = true;
					}

				}
			} finally {
				lock (initValLock) {
					initValBoolean = false;
					Monitor.PulseAll(initValLock);
				}
			}

		}

		public void WaitInitComplete() {
			try {
				lock (initValLock) {
					while (initValBoolean == true) {
						Monitor.Wait(initValLock);
					}
				}
			} catch (ThreadInterruptedException e) {
				Log.Error( "InterruptedException");
			}
		}

		public void ClearAllMachines() {
			lock (cluster) {
				cluster.Clear();
				cluster.Add(address);
				connected = false;
				Log.Info(String.Format("  **** Setting connected to false {0}", address));
			}
		}

		private void CheckClusterAvailability(int size) {
			lock (cluster) {
				// For all the machines in the cluster,
				int availableCount = 0;
				foreach (IServiceAddress m in cluster) {
					if (tracker.IsServiceUp(m, ServiceType.Manager)) {
						++availableCount;
					}
				}
				if (availableCount <= size/2) {
					connected = false;
					Log.Info(String.Format("  **** Setting connected to false {0}", address));
				}
			}
		}

		public void AddMachine(IServiceAddress addr) {
			lock (cluster) {
				int origSize = cluster.Count;

				if (cluster.Contains(addr))
					throw new ApplicationException("Machine already in cluster");

				cluster.Add(addr);

				CheckClusterAvailability(origSize);
			}
		}

		public void RemoveMachine(IServiceAddress addr) {
			bool removed;
			lock (cluster) {
				removed = cluster.Remove(addr);
				CheckClusterAvailability(cluster.Count);
			}
			if (!removed) {
				throw new ApplicationException("Machine not found in cluster");
			}
		}

		public static bool IsConnectionFault(Message m) {
			MessageError et = m.Error;
			// If it's a connect exception,
			string exType = et.ErrorType;
			if (exType.Equals("Sstem.Net.Sockets.SocketException")) {
				return true;
			} 
			if (exType.Equals("Deveel.Data.Net.ServiceNotConnectedException")) {
				return true;
			}
			return false;
		}

		private bool IsMajorityConnected() {
			// Check a majority of servers in the cluster are up.
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			int connectCount = 0;

			// For each machine
			foreach (IServiceAddress machine in machines) {
				// Check it's up
				if (tracker.IsServiceUp(machine, ServiceType.Manager)) {
					++connectCount;
				}
			}

			return connectCount > machines.Count/2;
		}

		public void CheckConnected() {
			// If less than a majority of servers are connected then throw an
			// exception,
			if (!connected || !IsMajorityConnected()) {
				throw new ServiceNotConnectedException("Manager service " + address +
				                                       " is not connected (majority = " + IsMajorityConnected() + ")");
			}
		}

		private LogEntryIterator RequestMessagesFrom(IServiceAddress machine, long[] latestUid) {
			// Return the iterator,
			return new LogEntryIterator(this, machine, latestUid);
		}

		private long PlaybackMessages(LogEntryIterator messages) {
			long count = 0;
			List<LogEntry> bundle = new List<LogEntry>(32);

			while (true) {

				bool done = false;
				bundle.Clear();
				for (int i = 0; i < 32 && !done; ++i) {
					LogEntry entry = messages.NextLogEntry();
					if (entry == null) {
						done = true;
					} else {
						bundle.Add(entry);
					}
				}

				// Finished,
				if (bundle.Count == 0 && done)
					break;

				lock (blockDbWriteLock) {
					ITransaction transaction = blockDatabase.CreateTransaction();
					try {
						foreach (LogEntry entry in bundle) {
							// Deserialize the entry,

							long[] uid = entry.Uid;
							byte[] buf = entry.Buffer;

							// If this hasn't applied the uid then we apply it,
							if (!HasAppliedUid(transaction, uid)) {

								MemoryStream bin = new MemoryStream(buf);
								BinaryReader din = new BinaryReader(bin, Encoding.Unicode);

								byte m = din.ReadByte();
								if (m == 18) {
									// Block_id to server map
									long blockIdH = din.ReadInt64();
									long blockIdL = din.ReadInt64();
									BlockId blockId = new BlockId(blockIdH, blockIdL);
									int sz = din.ReadInt32();
									long[] servers = new long[sz];
									for (int i = 0; i < sz; ++i) {
										servers[i] = din.ReadInt64();
									}

									// Replay this command,
									InsertBlockIdServerEntry(transaction, uid, blockId, servers);

								} else if (m == 19) {
									// Key/Value pair
									String key = din.ReadString();
									String value = null;
									byte vb = din.ReadByte();
									if (vb == 1) {
										value = din.ReadString();
									}

									// Replay this command,
									InsertKeyValueEntry(transaction, uid, key, value);

								} else {
									throw new ApplicationException("Unknown entry type: " + m);
								}

								// Increment the count,
								++count;

							}

						} // For each entry in the bundle

						// Commit and check point the update,
						blockDatabase.Publish(transaction);
						blockDatabase.CheckPoint();
					} catch (IOException e) {
						throw new ApplicationException(e.Message, e);
					} finally {
						blockDatabase.Dispose(transaction);
					}

				} // lock

			} // while true

			return count;
		}

		private int SendCommand(List<ServiceMessageQueue> pendingQueue, List<IServiceAddress> machines, MessageStream outputStream) {
			// Send the messages,
			IEnumerable<Message>[] input = new IEnumerable<Message>[machines.Count];

			// For each machine in the cluster, send the process commands,
			for (int x = 0; x < machines.Count; ++x) {
				IServiceAddress machine = machines[x];
				IEnumerable<Message> inputStream = null;
				// If the service is up,
				if (tracker.IsServiceUp(machine, ServiceType.Manager)) {
					// Send to the service,
					IMessageProcessor processor = connector.Connect(machine, ServiceType.Manager);
					inputStream = processor.Process(outputStream);
				}
				input[x] = inputStream;
			}

			// Now read in the results.

			int sendCount = 0;

			int i = 0;
			while (i < input.Length) {
				bool msgSent = true;
				// Null indicates not sent,
				if (input[i] == null) {
					msgSent = false;
				} else {
					IEnumerator<Message> msgs = input[i].GetEnumerator();
					while (msgs.MoveNext()) {
						Message m = msgs.Current;
						if (m.HasError) {
							// If it's not a comm fault, we throw the error now,
							if (!IsConnectionFault(m))
								throw new ApplicationException(m.ErrorMessage);

							// Inform the tracker of the fault,
							tracker.ReportServiceDownClientReport(machines[i], ServiceType.Manager);
							msgSent = false;
						}
					}
				}

				// If not sent, queue the message,
				if (!msgSent) {
					// The machine that is not available,
					IServiceAddress machine = machines[i];

					// Get the queue for the machine,
					ServiceMessageQueue queue = comm.CreateServiceMessageQueue();
					queue.AddMessageStream(machine, outputStream, ServiceType.Manager);

					// Add this queue to the pending queue list
					pendingQueue.Add(queue);
				} else {
					// Otherwise we sent the message with no error,
					++sendCount;
				}

				++i;
			}

			return sendCount;
		}

		private int SendProposalToNetwork(List<ServiceMessageQueue> pendingQueue, long[] uid, String key, String value) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			Message message = new Message("internalKVProposal", uid, key, value);

			// Send the proposal command out to the machines on the network,
			int sendCount = SendCommand(pendingQueue, machines, message.AsStream());

			// If we sent to a majority, return 1
			if (sendCount > machines.Count/2)
				return 1;

			// Otherwise return 2, (majority of machines in the cluster not available).
			return 2;
		}

		private void SendProposalComplete(List<ServiceMessageQueue> pendingQueue, long[] uid, String key, String value) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			Message message = new Message("internalKVComplete", uid, key, value);

			// Send the complete proposal message out to the machines on the network,
			SendCommand(pendingQueue, machines, message.AsStream());

			// Enqueue all pending messages,
			foreach (ServiceMessageQueue queue in pendingQueue) {
				queue.Enqueue();
			}
		}

		private int SendProposalToNetwork(List<ServiceMessageQueue> pendingQueue, long[] uid, BlockId blockId, long[] blockServerUids) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			Message message = new Message("internalBSProposal", uid, blockId, blockServerUids);

			// Send the proposal command out to the machines on the network,
			int sendCount = SendCommand(pendingQueue, machines, message.AsStream());

			// If we sent to a majority, return 1
			if (sendCount > machines.Count/2)
				return 1;

			// Otherwise return 2, (majority of machines in the cluster not available).
			return 2;
		}

		private void SendProposalComplete(List<ServiceMessageQueue> pendingQueue, long[] uid, BlockId blockId, long[] blockServerUids) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			Message message = new Message("internalBSComplete", uid, blockId, blockServerUids);

			// Send the complete proposal message out to the machines on the network,
			SendCommand(pendingQueue, machines, message.AsStream());

			// Enqueue all pending messages,
			foreach (ServiceMessageQueue queue in pendingQueue) {
				queue.Enqueue();
			}
		}

		private static string ToUidString(long uidHigh, long uidLow) {
			StringBuilder b = new StringBuilder();
			b.Append(Convert.ToString(uidHigh, 16));
			b.Append("-");
			b.Append(Convert.ToString(uidLow, 16));
			return b.ToString();
		}

		private static String ToUidString(long[] uid) {
			return ToUidString(uid[0], uid[1]);
		}

		private static long[] ParseUidString(String str) {
			int delim = str.IndexOf('-');
			if (delim == -1) {
				throw new ApplicationException("Format error");
			}
			long hv = Convert.ToInt64(str.Substring(0, delim), 16);
			long lv = Convert.ToInt64(str.Substring(delim + 1), 16);
			return new long[] {hv, lv};
		}

		private static long[] GenerateUid() {
			long timeMs = DateTimeUtil.CurrentTimeMillis();
			byte[] bytes = new byte[8];
			Random.GetBytes(bytes);
			long rv = BitConverter.ToInt64(bytes, 0);
			if (rv < 0) {
				rv = -rv;
			}

			return new long[] {timeMs, rv};
		}

		private void InternalKvProposal(long[] uid, String key, String value) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();
		}

		private void InternalKvComplete(long[] uid, String key, String value) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// We must handle the case when multiple identical proposals come in,
					if (!HasAppliedUid(transaction, uid)) {
						// Add the serialization to the transaction log,
						InsertKeyValueEntry(transaction, uid, key, value);

						// Commit and check point the update,
						blockDatabase.Publish(transaction);
						blockDatabase.CheckPoint();
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}


		}

		private void InternalBsProposal(long[] uid, BlockId blockId, long[] serverUids) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();
		}

		private void InternalBsComplete(long[] uid, BlockId blockId, long[] serverUids) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// We must handle the case when multiple identical proposals come in,
					if (!HasAppliedUid(transaction, uid)) {
						// Insert the block id server mapping,
						InsertBlockIdServerEntry(transaction, uid, blockId, serverUids);

						// Commit and check point the update,
						blockDatabase.Publish(transaction);
						blockDatabase.CheckPoint();
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		public void SetValue(string key, string value) {
			// If the given value is the same as the current value stored, return.
			string inValue = GetValue(key);
			if ((inValue == null && value == null) ||
			    (inValue != null && value != null && inValue.Equals(value))) {
				return;
			}

			// Sets a server_uids in the network. This performs the following operations;
			//
			// 1) Sends an 'internalProposeValue(uid, block_id, server_uids)' server_uids to all the
			//    currently available machines in the cluster.
			// 2) When a majority of machines have accepted the proposal, sends a
			//    complete message out to the network.

			// Each machine in the network does the following;
			// 1) When an 'internalProposeValue' command is received, puts the message
			//    in a queue.
			// 2) When a complete message is received, the message is written to the
			//    database.

			// Generate a UID string,
			long[] uid = GenerateUid();

			// Pending messages from connection faults,
			List<ServiceMessageQueue> pendingQueue = new List<ServiceMessageQueue>(7);

			// Send the proposal out to the network,
			int status = SendProposalToNetwork(pendingQueue, uid, key, value);

			// If a majority of machines accepted the proposal, send the complete
			// operation.
			if (status == 1) {
				SendProposalComplete(pendingQueue, uid, key, value);
			} else {
				// Otherwise generate the exception,
				throw new ApplicationException("A majority of the cluster is not available");
			}
		}

		public string GetValue(string key) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {

					long[] uid = GetUidForKey(transaction, key);
					if (uid == null) {
						// Return null if not found,
						return null;
					}
					byte[] buf = GetValueFromUid(transaction, uid);
					if (buf == null) {
						return null;
					}

					MemoryStream bin = new MemoryStream(buf);
					BinaryReader din = new BinaryReader(bin, Encoding.Unicode);

					byte m = din.ReadByte();
					String inKey = din.ReadString();
					String inValue = null;
					byte vb = din.ReadByte();
					if (vb == 1) {
						inValue = din.ReadString();
					}

					if (m != 19)
						throw new ApplicationException("Unexpected value marker");
					if (!inKey.Equals(key))
						throw new ApplicationException("Keys don't match");

					return inValue;
				} catch (IOException e) {
					throw new ApplicationException(e.Message, e);
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

		}

		public void SetBlockIdServerMap(BlockId blockId, long[] blockServerUids) {
			// Generate a UID string,
			long[] uid = GenerateUid();

			// Pending messages from connection faults,
			List<ServiceMessageQueue> pendingQueue = new List<ServiceMessageQueue>(7);

			// Send the proposal out to the network,
			int status = SendProposalToNetwork(pendingQueue, uid, blockId, blockServerUids);

			// If a majority of machines accepted the proposal, send the complete
			// operation.
			if (status == 1) {
				SendProposalComplete(pendingQueue, uid, blockId, blockServerUids);
			} else {
				// Otherwise generate the exception,
				throw new ApplicationException("A majority of the cluster is not available");
			}

		}

		public long[] GetBlockIdServerMap(BlockId blockId) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					long[] uid = GetUidForBlock(transaction, blockId);
					if (uid == null) {
						return new long[0];
					}
					byte[] buf = GetValueFromUid(transaction, uid);

					MemoryStream bin = new MemoryStream(buf);
					BinaryReader din = new BinaryReader(bin, Encoding.Unicode);

					// Deserialize the value,
					byte m = din.ReadByte();
					long blockIdH = din.ReadInt64();
					long blockIdL = din.ReadInt64();
					BlockId valBlockId = new BlockId(blockIdH, blockIdL);
					int sz = din.ReadInt32();
					long[] valServers = new long[sz];
					for (int i = 0; i < sz; ++i) {
						valServers[i] = din.ReadInt64();
					}

					// Sanity checks,
					if (m != 18)
						throw new ApplicationException("Unexpected value marker");
					if (!valBlockId.Equals(blockId))
						throw new ApplicationException("Block IDs don't match");

					return valServers;

				} catch (IOException e) {
					throw new ApplicationException(e.Message, e);
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}

		}

		public BlockId GetLastBlockId() {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					// Return the last block id
					return GetLastBlockId(transaction);
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		public string[] GetKeys(string prefix) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			lock (blockDbWriteLock) {
				ITransaction t = blockDatabase.CreateTransaction();

				try {
					IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
					StringDictionary keyList = new StringDictionary(keyListDf);
					ISortedCollection<string> keySet = keyList.Keys;

					if (prefix.Length > 0) {
						// Reduction,
						String firstItem = prefix;
						String lastItem = prefix.Substring(0, prefix.Length - 1) + (prefix[prefix.Length - 1] + 1);

						keySet = keySet.Sub(firstItem, lastItem);
					}

					// Make the array and return
					List<String> strKeysList = new List<string>(64);
					IEnumerator<String> i = keySet.GetEnumerator();
					while (i.MoveNext()) {
						strKeysList.Add(i.Current);
					}

					return strKeysList.ToArray();

				} finally {
					blockDatabase.Dispose(t);
				}
			}
		}

		private long[] GetLatestUid() {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					// Create the UIDList object,
					IDataFile uidListDf = transaction.GetFile(UidListKey, FileAccess.ReadWrite);
					UidList uidList = new UidList(uidListDf);

					return uidList.GetLastUid();

				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private void InternalFetchLogBundle(MessageStream replyMessage, long[] uid, bool initial) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (blockDbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					// Create the UIDList object,
					IDataFile uidListDf = transaction.GetFile(UidListKey, FileAccess.ReadWrite);
					UidList uidList = new UidList(uidListDf);

					// Go to the position of the uid,
					long pos = 0;
					if (uid != null) {
						pos = uidList.PositionOfUid(uid);
					}
					long end = Math.Min(pos + 32, uidList.Count);
					if (pos < 0) {
						pos = -(pos + 1);
					}

					// If initial is true, we go back a bit
					if (initial) {
						// Go back 16 entries in the log (but don't go back before the first)
						pos = Math.Max(0, (pos - 16));
					} else {
						// Go to the next entry,
						pos = pos + 1;
					}

					// Send the bundle out to the stream,
					for (long i = pos; i < end; ++i) {
						long[] inUid = uidList.GetUid(i);
						byte[] buf = GetValueFromUid(transaction, inUid);

						replyMessage.AddMessage(new Message(inUid, buf));
					}

				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		public void Process(Message m, MessageStream replyMessage) {
			String cmd = m.Name;

			if (cmd.Equals("internalKVProposal")) {
				long[] uid = (long[]) m.Arguments[0].Value;
				String key = (String) m.Arguments[1].Value;
				String value = (String) m.Arguments[2].Value;
				InternalKvProposal(uid, key, value);
				replyMessage.AddMessage(new Message(1));
			} else if (cmd.Equals("internalKVComplete")) {
				long[] uid = (long[]) m.Arguments[0].Value;
				String key = (String) m.Arguments[1].Value;
				String value = (String) m.Arguments[2].Value;
				InternalKvComplete(uid, key, value);
				replyMessage.AddMessage(new Message(1));
			} else if (cmd.Equals("internalBSProposal")) {
				long[] uid = (long[]) m.Arguments[0].Value;
				BlockId blockId = (BlockId) m.Arguments[1].Value;
				long[] serverUids = (long[]) m.Arguments[2].Value;
				InternalBsProposal(uid, blockId, serverUids);
				replyMessage.AddMessage(new Message(1));
			} else if (cmd.Equals("internalBSComplete")) {
				long[] uid = (long[]) m.Arguments[0].Value;
				BlockId blockId = (BlockId) m.Arguments[1].Value;
				long[] serverUids = (long[]) m.Arguments[2].Value;
				InternalBsComplete(uid, blockId, serverUids);
				replyMessage.AddMessage(new Message(1));
			} else if (cmd.Equals("internalFetchLogBundle")) {
				long[] uid = (long[]) m.Arguments[0].Value;
				bool initial = ((int) m.Arguments[1].Value) != 0;
				InternalFetchLogBundle(replyMessage, uid, initial);
			} else if (cmd.Equals("debugString")) {
				StringWriter strOut = new StringWriter();
				DebugOutput(strOut);
				strOut.Flush();
				replyMessage.AddMessage(new Message(strOut.ToString()));
			} else {
				throw new ApplicationException("Unknown command: " + m.Name);
			}
		}

		private void DebugOutput(StringWriter output) {
			//TODO:
		}

		// ------ Storage ------

		private static long[] GetUidForBlock(ITransaction t, BlockId blockId) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.ReadWrite);
			BlockIdUidList blockidUidList = new BlockIdUidList(bidUidListDf);

			long pos = blockidUidList.PositionOfBlockId(blockId);
			if (pos < 0)
				return null;

			return blockidUidList.GetUidAt(pos);
		}

		private static long[] GetUidForKey(ITransaction t, string key) {
			IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
			StringDictionary keyList = new StringDictionary(keyListDf);

			string val = keyList.GetValue(key);
			if (val == null) {
				return null;
			}
			return ParseUidString(val);
		}

		private bool HasAppliedUid(ITransaction t, long[] uid) {
			// Make a hash value,
			long hashCode = uid[0]/16;

			// Turn it into a key object,
			Key hashKey = new Key(13, 0, hashCode);

			// The DataFile
			IDataFile dfile = t.GetFile(hashKey, FileAccess.ReadWrite);

			long pos = 0;
			long size = dfile.Length;
			BinaryReader reader = new BinaryReader(new DataFileStream(dfile));
			while (pos < size) {
				dfile.Position = pos;
				int sz = reader.ReadInt32();

				// Get the stored uid,
				long inuidH = reader.ReadInt64();
				long inuidL = reader.ReadInt64();

				// If the uid matches,
				if (inuidH == uid[0] && inuidL == uid[1]) {
					// Match, so return true
					return true;
				}

				pos = pos + sz;
			}

			// Not found, return false
			return false;
		}

		private byte[] GetValueFromUid(ITransaction t, long[] uid) {

			// Make a hash value,
			long hashCode = uid[0]/16;

			// Turn it into a key object,
			Key hashKey = new Key(13, 0, hashCode);

			// The DataFile
			IDataFile dfile = t.GetFile(hashKey, FileAccess.ReadWrite);
			BinaryReader reader = new BinaryReader(new DataFileStream(dfile));

			long pos = 0;
			long size = dfile.Length;
			while (pos < size) {
				dfile.Position = pos;
				int sz = reader.ReadInt32();

				// Get the stored uid,
				long inuidH = reader.ReadInt64();
				long inuidL = reader.ReadInt64();

				// If the uid matches,
				if (inuidH == uid[0] && inuidL == uid[1]) {
					// Match, so put the serialization of the record in the file,
					int bufSz = sz - 20;
					byte[] buf = new byte[bufSz];
					reader.Read(buf, 0, bufSz);
					// If buf is empty return null,
					if (buf.Length == 0)
						return null;

					return buf;
				}

				pos = pos + sz;
			}

			// Not found, return null
			return null;
		}

		private BlockId GetLastBlockId(ITransaction t) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.Read);
			BlockIdUidList blockidUidList = new BlockIdUidList(bidUidListDf);

			long pos = blockidUidList.Count - 1;
			if (pos < 0) {
				return null;
			}

			return blockidUidList.GetBlockIdAt(pos);
		}

		private static void InsertToUidList(ITransaction t, long[] uid) {
			// Create the UIDList object,
			IDataFile uidListDf = t.GetFile(UidListKey, FileAccess.ReadWrite);
			UidList uidList = new UidList(uidListDf);

			// Inserts the uid in the list
			uidList.AddUid(uid);
		}

		private void InsertValue(ITransaction t, long[] uid, byte[] value) {
			// Make a hash value,
			long hashCode = uid[0]/16;

			// Insert the uid into a list,
			InsertToUidList(t, uid);

			// Turn it into a key object,
			Key hashKey = new Key(13, 0, hashCode);

			// The DataFile
			IDataFile dfile = t.GetFile(hashKey, FileAccess.ReadWrite);
			BinaryWriter writer = new BinaryWriter(new DataFileStream(dfile));
			// The size of the entry being added,
			int sz = 20 + value.Length;

			// Position at the end of the file,
			dfile.Position = dfile.Length;
			// Insert the entry,
			// Put the size of the value entry,
			writer.Write(sz);
			// The 128-bit uid,
			writer.Write(uid[0]);
			writer.Write(uid[1]);
			// The value content,
			writer.Write(value, 0, value.Length);
		}

		private static void InsertBlockIdRef(ITransaction t, BlockId blockId, long[] uid) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.ReadWrite);
			BlockIdUidList blockidUidList = new BlockIdUidList(bidUidListDf);

			blockidUidList.AddBlockIdRef(blockId, uid);
		}

		private void InsertKeyRef(ITransaction t, string key, string value, long[] uid) {
			IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
			StringDictionary keyList = new StringDictionary(keyListDf);

			// Put it in the property set,
			if (value == null) {
				keyList.SetValue(key, null);
			} else {
				keyList.SetValue(key, ToUidString(uid[0], uid[1]));
			}
		}

		private void InsertBlockIdServerEntry(ITransaction t, long[] uid, BlockId blockId, long[] servers) {
			byte[] buf;

			// Put this proposal in a local log,
			try {
				MemoryStream bout = new MemoryStream(64);
				BinaryWriter dout = new BinaryWriter(bout);
				dout.Write((byte)18);
				dout.Write(blockId.High);
				dout.Write(blockId.Low);
				dout.Write(servers.Length);
				for (int i = 0; i < servers.Length; ++i) {
					dout.Write(servers[i]);
				}

				buf = bout.ToArray();
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}

			// Inserts the value
			InsertValue(t, uid, buf);
			// Inserts a reference
			InsertBlockIdRef(t, blockId, uid);
		}

		private void InsertKeyValueEntry(ITransaction t, long[] uid, String key, String value) {
			byte[] buf;

			// Put this proposal in a local log,
			try {
				MemoryStream bout = new MemoryStream(256);
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
				dout.Write((byte)19);
				dout.Write(key);
				if (value == null) {
					dout.Write((byte)0);
				} else {
					dout.Write((byte)1);
					dout.Write(value);
				}

				buf = bout.ToArray();
			} catch (IOException e) {
				throw new ApplicationException(e.Message, e);
			}

			// Inserts the value
			InsertValue(t, uid, buf);
			// Inserts a reference
			InsertKeyRef(t, key, value, uid);
		}


		// ----------

		private class UidList : FixedSizeCollection {

			public UidList(IDataFile data)
				: base(data, 16) {
			}

			public long PositionOfUid(long[] uid) {
				return Search(new Quadruple(uid[0], uid[1]));
			}

			public long[] GetUid(long pos) {
				base.SetPosition(pos);
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new long[] {highV, lowV};
			}

			public long[] GetLastUid() {
				long pos = Count - 1;
				return pos < 0 ? null : GetUid(pos);
			}

			public void AddUid(long[] uid) {
				// Returns the position of the uid in the list
				long pos = PositionOfUid(uid);
				if (pos < 0) {
					pos = -(pos + 1);
					InsertEmptyRecord(pos);
					SetPosition(pos);
					Output.Write(uid[0]);
					Output.Write(uid[1]);
				} else {
					throw new ApplicationException("UID already in list");
				}
			}

			private Quadruple GetQuadrupleKey(long recordPos) {
				SetPosition(recordPos);
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new Quadruple(highV, lowV);
			}

			protected override object GetRecordKey(long recordPos) {
				return GetQuadrupleKey(recordPos);
			}

			protected override int CompareRecordTo(long recordPos, object recordKey) {
				Quadruple v1 = GetQuadrupleKey(recordPos);
				Quadruple v2 = (Quadruple) recordKey;
				return v1.CompareTo(v2);
			}

		}

		private class BlockIdUidList : FixedSizeCollection {
			public BlockIdUidList(IDataFile data)
				: base(data, 32) {
			}

			public long PositionOfBlockId(BlockId blockId) {
				return Search(blockId);
			}

			public void AddBlockIdRef(BlockId blockId, long[] uid) {
				// Returns the position of the uid in the list
				long pos = PositionOfBlockId(blockId);
				if (pos < 0) {
					pos = -(pos + 1);
					// Insert space for a new entry
					InsertEmptyRecord(pos);
					SetPosition(pos);
				} else {
					// Go to position to overwrite current value
					SetPosition(pos);
				}

				Output.Write(blockId.High);
				Output.Write(blockId.Low);
				Output.Write(uid[0]);
				Output.Write(uid[1]);
			}

			public BlockId GetBlockIdAt(long recordPos) {
				SetPosition(recordPos);
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new BlockId(highV, lowV);
			}

			public long[] GetUidAt(long recordPos) {
				SetPosition(recordPos);
				DataFile.Position = DataFile.Position + 16;
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new long[] {highV, lowV};
			}

			protected override object GetRecordKey(long recordPos) {
				return GetBlockIdAt(recordPos);
			}

			protected override int CompareRecordTo(long recordPos, object recordKey) {
				BlockId v1 = GetBlockIdAt(recordPos);
				BlockId v2 = (BlockId) recordKey;
				return v1.CompareTo(v2);
			}

		}

		private class LogEntryIterator {
			private readonly ReplicatedValueStore valueStore;
			private readonly IServiceAddress machine;
			private long[] firstUid; // The first UID of the next block
			private bool initial;
			private readonly List<LogEntry> logEntries;
			private int index;

			public LogEntryIterator(ReplicatedValueStore valueStore, IServiceAddress machine, long[] firstUid) {
				this.valueStore = valueStore;
				this.machine = machine;
				this.firstUid = firstUid;
				initial = true;
				logEntries = new List<LogEntry>(64);
				index = 0;
			}

			private void FetchNextBlock() {
				Message message = new Message("internalFetchLogBundle", firstUid, initial ? 1 : 0);

				// Clear the log entries,
				logEntries.Clear();
				index = 0;

				// Send the open stream command.
				// If the service is up,
				if (valueStore.tracker.IsServiceUp(machine, ServiceType.Manager)) {
					// Send to the service,
					IMessageProcessor processor = valueStore.connector.Connect(machine, ServiceType.Manager);
					IEnumerable<Message> response = processor.Process(message.AsStream());

					// If it's a connection error, return null,
					foreach (Message m in response) {
						if (m.HasError) {
							// Report the service down if connection failure
							if (IsConnectionFault(m)) {
								valueStore.tracker.ReportServiceDownClientReport(machine, ServiceType.Manager);
							}

							throw new ApplicationException(m.ErrorMessage);
						} else {
							long[] uid = (long[]) m.Arguments[0].Value;
							byte[] buf = (byte[]) m.Arguments[1].Value;
							logEntries.Add(new LogEntry(uid, buf));
						}
					}
				} else {
					throw new ApplicationException("Service down");
				}

				// Update the first uid of the next block,
				if (logEntries.Count > 0) {
					LogEntry lastEntry = logEntries[logEntries.Count - 1];
					firstUid = lastEntry.Uid;
				}

			}

			public LogEntry NextLogEntry() {
				// The end state,
				if (initial == false && index >= logEntries.Count) {
					return null;
				}

				if (initial) {
					FetchNextBlock();
					initial = false;
					// End reached?
					if (index >= logEntries.Count) {
						return null;
					}
				}

				// Get the entry from the bundle,
				LogEntry entry = logEntries[index];

				++index;
				// If we reached the end fetch the next bundle,
				if (index >= logEntries.Count) {
					FetchNextBlock();
				}

				return entry;
			}
		}

		private class LogEntry {

			private readonly long[] uid;
			private readonly byte[] buf;

			public LogEntry(long[] uid, byte[] buf) {
				this.uid = uid;
				this.buf = buf;
			}

			public long[] Uid {
				get { return uid; }
			}

			public byte[] Buffer {
				get { return buf; }
			}
		}
	}
}