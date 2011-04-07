using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class ReplicatedValueStore {
		private readonly IServiceAddress serviceAddress;
		private readonly IServiceConnector serviceConnector;
		private readonly IDatabase blockDatabase;
		private readonly object dbWriteLock;
		private readonly ServiceStatusTracker tracker;
		private Timer timer;
		private readonly MessageCommunicator comm;
		private readonly List<IServiceAddress> cluster;
		private readonly static RandomNumberGenerator random = new RNGCryptoServiceProvider();

		private volatile bool connected;
		private bool initFlag;
		private readonly object initFlagLock = new object();


		private static readonly Logger log = Logger.Network;

		private static readonly Key UidListKey = new Key(12, 0, 40);
		private static readonly Key BlockidUidMapKey = new Key(12, 0, 50);
		private static readonly Key KeyUidMapKey = new Key(12, 0, 60);

		internal ReplicatedValueStore(IServiceAddress serviceAddress, IServiceConnector serviceConnector, IDatabase blockDatabase, object dbWriteLock, ServiceStatusTracker tracker) {
			this.serviceAddress = serviceAddress;
			this.serviceConnector = serviceConnector;
			this.blockDatabase = blockDatabase;
			this.dbWriteLock = dbWriteLock;
			this.tracker = tracker;
			comm = new MessageCommunicator(serviceConnector, tracker);

			cluster = new List<IServiceAddress>(9);
			ClearAllMachines();

			// Add listener for service status updates,
			tracker.StatusChange += OnStatusChange;
		}

		private void OnStatusChange(object sender, ServiceStatusEventArgs args) {
			if (args.ServiceType == ServiceType.Manager) {
				// If we detected that a manager is now available,
				if (args.NewStatus == ServiceStatus.Up) {
					// If this is not connected, initialize,
					if (!connected) {
						Init();
					} else {
						// Retry any pending messages on this service,
						comm.RetryMessagesFor(args.ServiceAddress);
					}
				} else {
					// If it's connected,
					if (connected) {
						int cluster_size;
						lock (cluster) {
							cluster_size = cluster.Count;
						}
						// Set connected to false if availability check fails,
						CheckClusterAvailability(cluster_size);
					}
				}
				// If a manager server goes down,
			}
		}


		private readonly Object initLock = new Object();

		internal void Init() {
			lock (initFlagLock) {
				initFlag = true;
			}

			if (timer == null)
				timer = new Timer(DoInit, null, 0, 500);
		}

		private void DoInit(object state) {
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
						if (!machine.Equals(serviceAddress)) {
							// Request all messages from the machines log from the given last
							// update time.
							LogEntryEnumerator messages = RequestMessagesFrom(machine, GetLatestUID());

							if (messages != null) {
								log.Info(String.Format("Synchronizing with {0}", machine));

								try {
									long synchronizeCount = PlaybackMessages(messages);

									log.Info(String.Format("{0} synching with {1} complete, message count = {2}",
									                       new Object[] {serviceAddress.ToString(), machine.ToString(), synchronizeCount}));

									// Add the machine to the synchronized_on list
									synchronizedOn.Add(machine);
								} catch (ApplicationException e) {
									// If we failed to sync, report the manager down
									log.Info(String.Format("Failed synchronizing with {0}", machine));
									log.Warning("Sync Fail Error", e);
									tracker.ReportServiceDownClientReport(machine, ServiceType.Manager);
								}
							}
						}
					}

					// If we synchronized on a majority of the machines, set the connected
					// flag to true,
					if ((synchronizedOn.Count + 1) > machines.Count / 2) {
						log.Info(String.Format("  **** Setting connected to true {0}", serviceAddress));
						connected = true;
						return;
					}
				}
			} finally {
				lock (initFlagLock) {
					initFlag = false;
					Monitor.PulseAll(initFlagLock);
				}
			}

		}

		internal void WaitInitComplete() {
			try {
				lock (initFlagLock) {
					while (initFlag) {
						Monitor.Wait(initFlagLock);
					}
				}
			} catch (ThreadInterruptedException e) {
				log.Error("ThreadInterruptedException", e);
			}
		}

		internal void ClearAllMachines() {
			lock (cluster) {
				cluster.Clear();
				cluster.Add(serviceAddress);
				connected = false;
				log.Info(String.Format("  **** Setting connected to false {0}", serviceAddress));
			}
		}

		private void CheckClusterAvailability(int size) {
			lock (cluster) {
				// For all the machines in the cluster,
				int available_count = 0;
				foreach (IServiceAddress m in cluster) {
					if (tracker.IsServiceUp(m, ServiceType.Manager)) {
						++available_count;
					}
				}
				if (available_count <= size / 2) {
					connected = false;
					log.Info(String.Format("  **** Setting connected to false {0}", serviceAddress));
				}
			}
		}

		internal void AddMachine(IServiceAddress addr) {
			lock (cluster) {
				int origSize = cluster.Count;

				if (cluster.Contains(addr))
					throw new ApplicationException("Machine already in cluster");

				cluster.Add(addr);
				CheckClusterAvailability(origSize);
			}
		}

		internal void RemoveMachine(IServiceAddress addr) {
			bool removed;
			lock (cluster) {
				removed = cluster.Remove(addr);
				CheckClusterAvailability(cluster.Count);
			}

			if (!removed)
				throw new ApplicationException("Machine not found in cluster");
		}

		public static bool IsConnectionFault(Message m) {
			MessageError error = m.Error;
			// If it's a connect exception,
			String source = error.Source;
			if (source.Equals("System.Net.ConnectException"))
				return true;
			if (source.Equals("Deveel.Data.Net.ServiceNotConnectedException"))
				return true;
			return false;
		}

		private bool IsMajorityConnected() {
			// Check a majority of servers in the cluster are up.
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}
			int connect_count = 0;
			// For each machine
			foreach (IServiceAddress machine in machines) {
				// Check it's up
				if (tracker.IsServiceUp(machine, ServiceType.Manager)) {
					++connect_count;
				}
			}
			return connect_count > machines.Count/2;
		}

		internal void CheckConnected() {
			// If less than a majority of servers are connected then throw an
			// exception,
			if (!connected || !IsMajorityConnected()) {
				throw new ServiceNotConnectedException("Manager service " + serviceAddress + " is not connected (majority = " +
				                                       IsMajorityConnected() + ")");
			}
		}

		private LogEntryEnumerator RequestMessagesFrom(IServiceAddress machine, long[] latest_uid) {
			// Return the iterator,
			return new LogEntryEnumerator(this, machine, latest_uid);
		}

		private long PlaybackMessages(LogEntryEnumerator messages) {
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

				lock (dbWriteLock) {
					ITransaction transaction = blockDatabase.CreateTransaction();
					try {
						foreach (LogEntry entry in bundle) {
							// Deserialize the entry,

							long[] uid = entry.UID;
							byte[] buf = entry.Buffer;

							// If this hasn't applied the uid then we apply it,
							if (!HasAppliedUID(transaction, uid)) {
								MemoryStream bin = new MemoryStream(buf);
								BinaryReader reader = new BinaryReader(bin);

								byte m = reader.ReadByte();
								if (m == 18) {
									// Block_id to server map
									long blockIdH = reader.ReadInt64();
									long blockIdL = reader.ReadInt64();
									BlockId blockId = new BlockId(blockIdH, blockIdL);
									int sz = reader.ReadInt32();
									long[] servers = new long[sz];
									for (int i = 0; i < sz; ++i) {
										servers[i] = reader.ReadInt64();
									}

									// Replay this command,
									InsertBlockIdServerEntry(transaction, uid, blockId, servers);
								} else if (m == 19) {
									// Key/Value pair
									string key = reader.ReadString();
									string value = null;
									byte vb = reader.ReadByte();
									if (vb == 1)
										value = reader.ReadString();

									// Replay this command,
									InsertKeyValueEntry(transaction, uid, key, value);
								} else {
									throw new ApplicationException("Unknown entry type: " + m);
								}

								// Increment the count,
								++count;
							}
						}  // For each entry in the bundle

						// Commit and check point the update,
						blockDatabase.Publish(transaction);
						blockDatabase.CheckPoint();
					} catch (IOException e) {
						throw new ApplicationException(e.Message, e);
					} finally {
						blockDatabase.Dispose(transaction);
					}
				}  // synchronized
			}  // while true

			return count;
		}

		private int SendCommand(List<ServiceMessageQueue> pendingQueue, List<IServiceAddress> machines, Message request) {
			// Send the messages,
			Message[] input = new Message[machines.Count];

			// For each machine in the cluster, send the process commands,
			for (int j = 0; j < machines.Count; ++j) {
				IServiceAddress machine = machines[j];
				Message response = null;
				// If the service is up,
				if (tracker.IsServiceUp(machine, ServiceType.Manager)) {
					// Send to the service,
					IMessageProcessor processor = serviceConnector.Connect(machine, ServiceType.Manager);
					response = processor.Process(request);
				}
				input[j] = response;
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
					Message m = input[i];
					if (m.HasError) {
						// If it's not a comm fault, we throw the error now,
						if (!IsConnectionFault(m)) {
							throw new ApplicationException(m.ErrorMessage);
						}
						// Inform the tracker of the fault,
						tracker.ReportServiceDownClientReport(machines[i], ServiceType.Manager);
						msgSent = false;
					}
				}

				// If not sent, queue the message,
				if (!msgSent) {
					// The machine that is not available,
					IServiceAddress machine = machines[i];

					// Get the queue for the machine,
					ServiceMessageQueue queue = comm.CreateServiceMessageQueue();
					queue.AddMessage(machine, ServiceType.Manager, request);

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

		private int SendProposalToNetwork(List<ServiceMessageQueue> pendingQueue, long[] uid, string key, string value) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			RequestMessage request = new RequestMessage("internalKVProposal");
			request.Arguments.Add(uid);
			request.Arguments.Add(key);
			request.Arguments.Add(value);

			// Send the proposal command out to the machines on the network,
			int sendCount = SendCommand(pendingQueue, machines, request);

			// If we sent to a majority, return 1
			if (sendCount > machines.Count/2)
				return 1;

			// Otherwise return 2, (majority of machines in the cluster not available).
			return 2;
		}

		private void SendProposalComplete(List<ServiceMessageQueue> pendingQueue, long[] uid, string key, string value) {
			List<IServiceAddress> machines = new List<IServiceAddress>(17);
			lock (cluster) {
				machines.AddRange(cluster);
			}

			// Create the message,
			RequestMessage request = new RequestMessage("internalKVComplete");
			request.Arguments.Add(uid);
			request.Arguments.Add(key);
			request.Arguments.Add(value);

			// Send the complete proposal message out to the machines on the network,
			SendCommand(pendingQueue, machines, request);

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
			RequestMessage request = new RequestMessage("internalBSProposal");
			request.Arguments.Add(uid);
			request.Arguments.Add(blockId);
			request.Arguments.Add(blockServerUids);

			// Send the proposal command out to the machines on the network,
			int send_count = SendCommand(pendingQueue, machines, request);

			// If we sent to a majority, return 1
			if (send_count > machines.Count/2)
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
			RequestMessage request = new RequestMessage("internalBSComplete");
			request.Arguments.Add(uid);
			request.Arguments.Add(blockId);
			request.Arguments.Add(blockServerUids);

			// Send the complete proposal message out to the machines on the network,
			SendCommand(pendingQueue, machines, request);

			// Enqueue all pending messages,
			foreach (ServiceMessageQueue queue in pendingQueue) {
				queue.Enqueue();
			}
		}

		private static String ToUIDString(long uid_high, long uid_low) {
			StringBuilder b = new StringBuilder();
			b.Append(Convert.ToString(uid_high, 32));
			b.Append("-");
			b.Append(Convert.ToString(uid_low, 32));
			return b.ToString();
		}

		private static String ToUIDString(long[] uid) {
			return ToUIDString(uid[0], uid[1]);
		}

		private static long[] ParseUIDString(String str) {
			int delim = str.IndexOf("-");
			if (delim == -1)
				throw new ApplicationException("Format error");

			long hv = Convert.ToInt64(str.Substring(0, delim), 32);
			long lv = Convert.ToInt64(str.Substring(delim + 1), 32);
			return new long[] { hv, lv };
		}

		private static long[] GenerateUID() {
			long time_ms = (long)(new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds);
			byte[] data = new byte[8];
			random.GetBytes(data);
			long rv = BitConverter.ToInt64(data, 0);
			if (rv < 0) {
				rv = -rv;
			}

			return new long[] { time_ms, rv };
		}

		private void InternalKvProposal(long[] uid, string key, string value) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();
		}

		private void InternalKvComplete(long[] uid, string key, string value) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// We must handle the case when multiple identical proposals come in,
					if (!HasAppliedUID(transaction, uid)) {
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
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();
				try {
					// We must handle the case when multiple identical proposals come in,
					if (!HasAppliedUID(transaction, uid)) {
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

		internal void SetValue(string key, string value) {
			// If the given value is the same as the current value stored, return.
			string in_value = GetValue(key);
			if ((in_value == null && value == null) ||
				(in_value != null && value != null && in_value.Equals(value))) {
				return;
			}

			// Sets a server_uids in the network. This performs the following operations;
			//
			// 1) Sends an 'InternalProposeValue(uid, blockId, serverUids)' server uids to all the
			//    currently available machines in the cluster.
			// 2) When a majority of machines have accepted the proposal, sends a
			//    complete message out to the network.

			// Each machine in the network does the following;
			// 1) When an 'InternalProposeValue' command is received, puts the message
			//    in a queue.
			// 2) When a complete message is received, the message is written to the
			//    database.

			// Generate a UID string,
			long[] uid = GenerateUID();

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

		internal string GetValue(String key) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					long[] uid = GetUIDForKey(transaction, key);
					if (uid == null)
						// Return null if not found,
						return null;

					byte[] buf = GetValueFromUID(transaction, uid);
					if (buf == null)
						return null;

					MemoryStream bin = new MemoryStream(buf);
					BinaryReader reader = new BinaryReader(bin);

					byte m = reader.ReadByte();
					string inKey = reader.ReadString();
					string inValue = null;
					byte vb = reader.ReadByte();
					if (vb == 1)
						inValue = reader.ReadString();

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

		internal void SetBlockIdServerMap(BlockId blockId, long[] blockServerUids) {
			// Generate a UID string,
			long[] uid = GenerateUID();

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

		internal long[] GetBlockIdServerMap(BlockId blockId) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					long[] uid = GetUIDForBlock(transaction, blockId);
					if (uid == null)
						return new long[0];

					byte[] buf = GetValueFromUID(transaction, uid);

					MemoryStream bin = new MemoryStream(buf);
					BinaryReader reader = new BinaryReader(bin);

					// Deserialize the value,
					byte m = reader.ReadByte();
					long blockIdH = reader.ReadInt64();
					long blockIdL = reader.ReadInt64();
					BlockId valBlockId = new BlockId(blockIdH, blockIdL);
					int sz = reader.ReadInt32();
					long[] valServers = new long[sz];
					for (int i = 0; i < sz; ++i) {
						valServers[i] = reader.ReadInt64();
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

		internal BlockId GetLastBlockId() {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
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

		internal String[] GetAllKeys(string prefix) {
			// Check this store is connected. Throws an exception if not.
			CheckConnected();

			lock (dbWriteLock) {
				ITransaction t = blockDatabase.CreateTransaction();

				try {
					IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
					StringDictionary keyList = new StringDictionary(keyListDf);
					ISortedCollection<string> keySet = keyList.Keys;

					if (prefix.Length > 0) {
						// Reduction,
						string firstItem = prefix;
						string lastItem = prefix.Substring(0, prefix.Length - 1) + (prefix[prefix.Length - 1] + 1);

						keySet = keySet.Sub(firstItem, lastItem);
					}

					// Make the array and return
					List<String> strKeysList = new List<string>(64);
					foreach (string s in keySet) {
						strKeysList.Add(s);
					}

					return strKeysList.ToArray();
				} finally {
					blockDatabase.Dispose(t);
				}
			}
		}

		private long[] GetLatestUID() {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					// Create the UIDList object,
					IDataFile uidListDf = transaction.GetFile(UidListKey, FileAccess.ReadWrite);
					UIDList uidList = new UIDList(uidListDf);

					return uidList.LastUID;
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		private void InternalFetchLogBundle(Message replyMessage, long[] uid, bool initial) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction transaction = blockDatabase.CreateTransaction();

				try {
					// Create the UIDList object,
					IDataFile uidListDf = transaction.GetFile(UidListKey, FileAccess.ReadWrite);
					UIDList uidList = new UIDList(uidListDf);

					// Go to the position of the uid,
					long pos = 0;
					if (uid != null)
						pos = uidList.IndexOfUID(uid);

					long end = Math.Min(pos + 32, uidList.Count);
					if (pos < 0)
						pos = -(pos + 1);

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
						long[] in_uid = uidList.GetUID(i);
						byte[] buf = GetValueFromUID(transaction, in_uid);

						replyMessage.Arguments.Add(in_uid);
						replyMessage.Arguments.Add(buf);
					}
				} finally {
					blockDatabase.Dispose(transaction);
				}
			}
		}

		public void Process(Message request, Message replyMessage) {
			string cmd = request.Name;

			switch (request.Name) {
				case "internalKVProposal": {
					long[] uid = (long[]) request.Arguments[0].Value;
					string key = request.Arguments[1].ToString();
					string value = request.Arguments[2].ToString();
					InternalKvProposal(uid, key, value);
					replyMessage.Arguments.Add(1);
					break;
				}
				case "internalKVComplete": {
					long[] uid = (long[]) request.Arguments[0].Value;
					string key = request.Arguments[1].ToString();
					string value = request.Arguments[2].ToString();
					InternalKvComplete(uid, key, value);
					replyMessage.Arguments.Add(1);
					break;
				}
				case "internalBSProposal": {
					long[] uid = (long[]) request.Arguments[0].Value;
					BlockId blockId = (BlockId) request.Arguments[1].Value;
					long[] server_uids = (long[]) request.Arguments[2].Value;
					InternalBsProposal(uid, blockId, server_uids);
					replyMessage.Arguments.Add(1);
					break;
				}
				case "internalBSComplete": {
					long[] uid = (long[]) request.Arguments[0].Value;
					BlockId blockId = (BlockId) request.Arguments[1].Value;
					long[] serverUids = (long[]) request.Arguments[2].Value;
					InternalBsComplete(uid, blockId, serverUids);
					replyMessage.Arguments.Add(1);
					break;
				}
				case "internalFetchLogBundle": {
					long[] uid = (long[]) request.Arguments[0].Value;
					bool initial = request.Arguments[1].ToInt32() != 0;
					InternalFetchLogBundle(replyMessage, uid, initial);
					break;
				}
				case "debugString": {
					StringWriter str_out = new StringWriter();
					DebugOutput(str_out);
					str_out.Flush();
					replyMessage.Arguments.Add(str_out.ToString());
					break;
				}
				default:
					throw new ApplicationException("Unknown command: " + request.Name);
			}
		}

		// ------ Storage ------

		public void DebugOutput(TextWriter output) {
			// Perform this under a lock. This lock is also active for block queries
			// and administration updates.
			lock (dbWriteLock) {
				// Create a transaction
				ITransaction t = blockDatabase.CreateTransaction();

				try {
					IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.ReadWrite);
					BlockIdUIDList blockidUidList = new BlockIdUIDList(bidUidListDf);
					IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
					StringDictionary keyList = new StringDictionary(keyListDf);
					IDataFile uidListDf = t.GetFile(UidListKey, FileAccess.ReadWrite);
					UIDList uidList = new UIDList(uidListDf);

					long size = blockidUidList.Count;
					output.Write("BlockID -> UID");
					output.WriteLine();
					for (long i = 0; i < size; ++i) {
						output.Write("  ");
						BlockId blockId = blockidUidList.GetBlockId(i);
						long[] uid = blockidUidList.GetUID(i);
						output.Write(blockId);
						output.Write(" -> ");
						output.WriteLine(ToUIDString(uid[0], uid[1]));
					}
					output.WriteLine();

					output.WriteLine("Key(String) -> UID");
					output.WriteLine();
					ISortedCollection<string> keys = keyList.Keys;
					foreach (String key in keys) {
						output.Write("  ");
						string uidString = keyList.GetValue(key);
						output.Write(key);
						output.Write(" -> ");
						output.WriteLine(uidString);
					}
					output.WriteLine();

					size = uidList.Count;
					output.WriteLine("UID");
					output.WriteLine();
					for (long i = 0; i < size; ++i) {
						output.Write("  ");
						long[] uid = uidList.GetUID(i);
						output.WriteLine(ToUIDString(uid[0], uid[1]));
					}
					output.WriteLine();
				} finally {
					blockDatabase.Dispose(t);
				}
			}
		}

		private long[] GetUIDForBlock(ITransaction t, BlockId blockId) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.ReadWrite);
			BlockIdUIDList blockidUidList = new BlockIdUIDList(bidUidListDf);

			long pos = blockidUidList.IndexOfBlockId(blockId);
			if (pos < 0)
				return null;
				
			return blockidUidList.GetUID(pos);
		}

		private long[] GetUIDForKey(ITransaction t, string key) {
			IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
			StringDictionary keyList = new StringDictionary(keyListDf);

			string val = keyList.GetValue(key);
			if (val == null)
				return null;

			return ParseUIDString(val);
		}

		private bool HasAppliedUID(ITransaction t, long[] uid) {
			// Make a hash value,
			long hashCode = uid[0] / 16;

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
					// Match, so return true
					return true;
				}

				pos = pos + sz;
			}

			// Not found, return false
			return false;
		}

		private byte[] GetValueFromUID(ITransaction t, long[] uid) {
			// Make a hash value,
			long hashCode = uid[0] / 16;

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
					int buf_sz = sz - 20;
					byte[] buf = new byte[buf_sz];
					dfile.Read(buf, 0, buf_sz);
					// If buf is empty return null,
					if (buf.Length == 0) {
						return null;
					}
					return buf;
				}

				pos = pos + sz;
			}

			// Not found, return null
			return null;
		}

		private BlockId GetLastBlockId(ITransaction t) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.Read);
			BlockIdUIDList blockidUidList = new BlockIdUIDList(bidUidListDf);

			long pos = blockidUidList.Count - 1;
			if (pos < 0)
				return null;

			return blockidUidList.GetBlockId(pos);
		}

		private void InsertToUIDList(ITransaction t, long[] uid) {
			// Create the UIDList object,
			IDataFile uidListDf = t.GetFile(UidListKey, FileAccess.ReadWrite);
			UIDList uidList = new UIDList(uidListDf);

			// Inserts the uid in the list
			uidList.AddUID(uid);
		}

		private void InsertValue(ITransaction t, long[] uid, byte[] value) {
			// Make a hash value,
			long hashCode = uid[0] / 16;

			// Insert the uid into a list,
			InsertToUIDList(t, uid);

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
			dfile.Write(value, 0, value.Length);
		}

		private void InsertBlockIdRef(ITransaction t, BlockId block_id, long[] uid) {
			IDataFile bidUidListDf = t.GetFile(BlockidUidMapKey, FileAccess.ReadWrite);
			BlockIdUIDList blockidUidList = new BlockIdUIDList(bidUidListDf);

			blockidUidList.AddBlockIdRef(block_id, uid);
		}

		private void InsertKeyRef(ITransaction t, string key, string value, long[] uid) {
			IDataFile keyListDf = t.GetFile(KeyUidMapKey, FileAccess.ReadWrite);
			StringDictionary keyList = new StringDictionary(keyListDf);

			// Put it in the property set,
			if (value == null) {
				keyList.SetValue(key, null);
			} else {
				keyList.SetValue(key, ToUIDString(uid[0], uid[1]));
			}
		}

		private void InsertBlockIdServerEntry(ITransaction t, long[] uid, BlockId blockId, long[] servers) {
			byte[] buf;

			// Put this proposal in a local log,
			try {
				MemoryStream bout = new MemoryStream(64);
				BinaryWriter writer = new BinaryWriter(bout);
				writer.Write((byte)18);
				writer.Write(blockId.High);
				writer.Write(blockId.Low);
				writer.Write(servers.Length);
				for (int i = 0; i < servers.Length; ++i) {
					writer.Write(servers[i]);
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

		private void InsertKeyValueEntry(ITransaction t, long[] uid, string key, string value) {
			byte[] buf;

			// Put this proposal in a local log,
			try {
				MemoryStream bout = new MemoryStream(256);
				BinaryWriter writer = new BinaryWriter(bout);
				writer.Write((byte)19);
				writer.Write(key);
				if (value == null) {
					writer.Write((byte)0);
				} else {
					writer.Write((byte)1);
					writer.Write(value);
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

		#region UIDList

		private class UIDList : FixedSizeCollection {
			public UIDList(IDataFile data)
				: base(data, 16) {
			}

			public long IndexOfUID(long[] uid) {
				return Search(new Quadruple(uid[0], uid[1]));
			}

			public long[] GetUID(long pos) {
				SetPosition(pos);
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new long[] { highV, lowV };
			}

			public long[] LastUID {
				get {
					long pos = Count - 1;
					return pos < 0 ? null : GetUID(pos);
				}
			}

			public void AddUID(long[] uid) {
				// Returns the position of the uid in the list
				long pos = IndexOfUID(uid);
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

			private Quadruple GetInt128BitKey(long recordPos) {
				SetPosition(recordPos);
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new Quadruple(highV, lowV);
			}

			protected override Object GetRecordKey(long recordPos) {
				return GetInt128BitKey(recordPos);
			}

			protected override int CompareRecordTo(long recordPos, object recordKey) {
				Quadruple v1 = GetInt128BitKey(recordPos);
				Quadruple v2 = (Quadruple)recordKey;
				return v1.CompareTo(v2);
			}

		}

		#endregion

		#region BlockIdUIDList

		private class BlockIdUIDList : FixedSizeCollection {

			public BlockIdUIDList(IDataFile data)
				: base(data, 32) {
			}

			public long IndexOfBlockId(BlockId block_id) {
				return Search(block_id);
			}

			public void AddBlockIdRef(BlockId blockId, long[] uid) {
				// Returns the position of the uid in the list
				long pos = IndexOfBlockId(blockId);
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

			public BlockId GetBlockId(long recordPos) {
				SetPosition(recordPos);

				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new BlockId(highV, lowV);
			}

			public long[] GetUID(long recordPos) {
				SetPosition(recordPos);

				DataFile.Position = DataFile.Position + 16;
				long highV = Input.ReadInt64();
				long lowV = Input.ReadInt64();
				return new long[] { highV, lowV };
			}

			protected override object GetRecordKey(long record_pos) {
				return GetBlockId(record_pos);
			}

			protected override int CompareRecordTo(long record_pos, Object record_key) {
				BlockId v1 = GetBlockId(record_pos);
				BlockId v2 = (BlockId)record_key;
				return v1.CompareTo(v2);
			}
		}

		#endregion

		#region LogEntryIterator

		private class LogEntryEnumerator {
			private readonly ReplicatedValueStore store;
			private readonly IServiceAddress machine;
			private long[] initialUid;          // The first UID of the next block
			private bool initial;
			private readonly List<LogEntry> logEntries;
			private int index;

			public LogEntryEnumerator(ReplicatedValueStore store, IServiceAddress machine, long[] initialUid) {
				this.store = store;
				this.machine = machine;
				this.initialUid = initialUid;
				initial = true;
				logEntries = new List<LogEntry>(64);
				index = 0;
			}

			private void FetchNextBlock() {
				MessageStream messageStream = new MessageStream(MessageType.Request);
				RequestMessage request = new RequestMessage("internalFetchLogBundle");
				request.Arguments.Add(initialUid);
				request.Arguments.Add(initial ? 1 : 0);
				messageStream.AddMessage(request);

				// Clear the log entries,
				logEntries.Clear();
				index = 0;

				// If the service is up,
				if (store.tracker.IsServiceUp(machine, ServiceType.Manager)) {
					// Send to the service,
					IMessageProcessor processor = store.serviceConnector.Connect(machine, ServiceType.Manager);
					// Send the open stream command.
					MessageStream response = (MessageStream) processor.Process(messageStream);

					// If it's a connection error, return null,
					foreach (Message m in response) {
						if (m.HasError) {
							// Report the service down if connection failure
							if (IsConnectionFault(m))
								store.tracker.ReportServiceDownClientReport(machine, ServiceType.Manager);

							Console.Out.WriteLine(m.ErrorStackTrace);
							throw new ApplicationException(m.ErrorMessage);
						}

						long[] uid = (long[]) m.Arguments[0].Value;
						byte[] buf = (byte[]) m.Arguments[1].Value;
						logEntries.Add(new LogEntry(uid, buf));
					}
				} else {
					throw new ApplicationException("Service down");
				}

				// Update the first uid of the next block,
				if (logEntries.Count > 0) {
					LogEntry lastEntry = logEntries[logEntries.Count - 1];
					initialUid = lastEntry.UID;
				}
			}

			public LogEntry NextLogEntry() {
				// The end state,
				if (initial == false && index >= logEntries.Count)
					return null;

				if (initial) {
					FetchNextBlock();
					initial = false;
					// End reached?
					if (index >= logEntries.Count)
						return null;
				}

				// Get the entry from the bundle,
				LogEntry entry = logEntries[index];

				++index;
				// If we reached the end fetch the next bundle,
				if (index >= logEntries.Count)
					FetchNextBlock();

				return entry;
			}
		}

		#endregion

		#region LogEntry

		private class LogEntry {

			private readonly long[] uid;
			private readonly byte[] buf;

			public LogEntry(long[] uid, byte[] buf) {
				this.uid = uid;
				this.buf = buf;
			}

			public long[] UID {
				get { return uid; }
			}

			public byte[] Buffer {
				get { return buf; }
			}
		}

		#endregion
	}
}