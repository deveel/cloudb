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
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public class MessageCommunicator {
		private readonly IServiceConnector network;
		private readonly ServiceStatusTracker tracker;
		private readonly Dictionary<IServiceAddress, RetryMessageQueue> queueMap;


		private static readonly Logger log = Logger.Network;

		internal MessageCommunicator(IServiceConnector network, ServiceStatusTracker tracker) {
			this.network = network;
			this.tracker = tracker;

			queueMap = new Dictionary<IServiceAddress, RetryMessageQueue>();
		}

		private void RetryMessageTask(object state) {
			IServiceAddress address = (IServiceAddress) state;

			List<ServiceType> types;
			List<MessageStream> messages;
			lock (queueMap) {
				// Remove from the queue,
				RetryMessageQueue queue;
				queueMap.TryGetValue(address, out queue);
				queueMap.Remove(address);
				types = queue.ServiceTypes;
				messages = queue.Queue;
			}
			// Create a message queue
			ServiceMessageQueue messageQueue = CreateServiceMessageQueue();
			// For each message in the queue,
			int sz = types.Count;
			for (int i = 0; i < sz; ++i) {
				ServiceType type = types[i];
				MessageStream messageStream = messages[i];

				if (type == ServiceType.Manager) {
					SendManagerMessage(messageQueue, address, messageStream);
				} else if (type == ServiceType.Root) {
					SendRootMessage(messageQueue, address, messageStream);
				} else {
					throw new ApplicationException("Unknown type");
				}
			}
			// Re-enqueue any pending messages,
			messageQueue.Enqueue();
		}

		internal void RetryMessagesFor(IServiceAddress address) {
			RetryMessageQueue queue;
			lock (queueMap) {
				queueMap.TryGetValue(address, out queue);
			}

			if (queue != null) {

				// Schedule on the timer queue,
				new Timer(RetryMessageTask, null, 500, Timeout.Infinite);

			}
		}

		private RetryMessageQueue GetRetryMessageQueue(IServiceAddress serviceAddress) {
			lock (queueMap) {
				RetryMessageQueue queue;
				if (!queueMap.TryGetValue(serviceAddress, out queue)) {
					queue = new RetryMessageQueue(serviceAddress);
					queueMap[serviceAddress] = queue;
				}
				return queue;
			}

		}

		private int SendRootMessage(ServiceMessageQueue queue, IServiceAddress rootServer, MessageStream messageOut) {
			int successCount = 0;

			// Process the message,
			IMessageProcessor processor = network.Connect(rootServer, ServiceType.Root);
			IEnumerable<Message> messageIn = processor.Process(messageOut);

			// Handle the response,
			foreach (Message m in messageIn) {
				if (m.HasError) {
					log.Error("Root error: " + m.ErrorMessage);

					// If we failed, add the message to the retry queue,
					if (queue != null) {
						queue.AddMessageStream(rootServer, messageOut, ServiceType.Root);
					}

					// TODO: We should confirm the error is a connection failure before
					//   reporting the service as down.
					// Report the service as down to the tracker,
					tracker.ReportServiceDownClientReport(rootServer, ServiceType.Root);
				} else {
					// Message successfully sent,
					++successCount;
				}
			}

			return successCount;
		}

		private int SendManagerMessage(ServiceMessageQueue queue, IServiceAddress managerServer, MessageStream messageOut) {
			int successCount = 0;

			// Process the message,
			IMessageProcessor processor = network.Connect(managerServer, ServiceType.Manager);
			IEnumerable<Message> messageIn = processor.Process(messageOut);

			// Handle the response,
			foreach (Message m in messageIn) {
				if (m.HasError) {
					log.Error("Manager error: " + m.ErrorMessage);

					// If we failed, add the message to the retry queue,
					if (queue != null) {
						queue.AddMessageStream(managerServer, messageOut, ServiceType.Manager);
					}
					// Report the service as down to the tracker,
					tracker.ReportServiceDownClientReport(managerServer, ServiceType.Manager);
				} else {
					// Message successfully sent,
					++successCount;
				}
			}

			return successCount;
		}

		internal ServiceMessageQueue CreateServiceMessageQueue() {
			return new MCServiceMessageQueue(this);
		}

		private class RetryMessageQueue {
			private readonly IServiceAddress serviceAddr;
			public readonly List<MessageStream> Queue;
			public readonly List<ServiceType> ServiceTypes;

			public RetryMessageQueue(IServiceAddress addr) {
				serviceAddr = addr;
				Queue = new List<MessageStream>();
				ServiceTypes = new List<ServiceType>();
			}

			public void add(MessageStream messageStream, ServiceType type) {
				lock (Queue) {
					Queue.Add(messageStream);
					ServiceTypes.Add(type);
					if (log.IsInterestedIn(LogLevel.Information)) {
						log.Info(String.Format("Queuing message {0} to service {1} {2}", messageStream, serviceAddr, type));
					}
				}
			}

		}

		private class MCServiceMessageQueue : ServiceMessageQueue {
			private readonly MessageCommunicator communicator;

			public MCServiceMessageQueue(MessageCommunicator communicator) {
				this.communicator = communicator;
			}

			public override void Enqueue() {
				int sz = ServiceAddresses.Count;
				for (int i = 0; i < sz; ++i) {
					IServiceAddress service_address = ServiceAddresses[i];
					MessageStream message_stream = Messages[i];
					ServiceType service_type = Types[i];

					communicator.GetRetryMessageQueue(service_address).add(message_stream, service_type);
				}
			}
		}
	}
}