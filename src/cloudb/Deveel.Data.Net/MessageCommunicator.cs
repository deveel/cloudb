using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public class MessageCommunicator {
		private readonly IServiceConnector connector;
		private readonly ServiceStatusTracker tracker;

		private Timer timer;
		private readonly Dictionary<IServiceAddress, RetryMessageQueue> queueMap;

		private static readonly Logger log = Logger.Network;


		internal MessageCommunicator(IServiceConnector connector, ServiceStatusTracker tracker) {
			this.connector = connector;
			this.tracker = tracker;
			queueMap = new Dictionary<IServiceAddress, RetryMessageQueue>();
		}

		private void Dequeue(object state) {
			IServiceAddress address = (IServiceAddress) state;
			List<ServiceType> types = null;
			List<Message> messages = null;
			lock (queueMap) {
				// Remove from the queue,
				RetryMessageQueue queue;
				if (queueMap.TryGetValue(address, out queue)) {
					queueMap.Remove(address);
					types = queue.serviceTypes;
					messages = queue.queue;
				}
			}

			if (types != null) {
				// Create a message queue
				ServiceMessageQueue message_queue = CreateServiceMessageQueue();
				// For each message in the queue,
				int sz = types.Count;
				for (int i = 0; i < sz; ++i) {
					ServiceType type = types[i];
					Message message_stream = messages[i];

					SendMessage(message_queue, address, type, message_stream);
				}

				// Re-enqueue any pending messages,
				message_queue.Enqueue();
			}
		}

		internal void RetryMessagesFor(IServiceAddress address) {
			bool hasQueue;
			lock (queueMap) {
				hasQueue = queueMap.ContainsKey(address);
			}

			if (hasQueue) {
				if (timer == null) {
					timer = new Timer(Dequeue, address, 500, Timeout.Infinite);
				} else {
					timer.Change(500, Timeout.Infinite);
				}
			}
		}

		/*
		 * TEMPORARILY COMMENTED
  void retryMessagesFor(IServiceAddress address) {
    RetryMessageQueue queue = null;
    lock (queue_map) {
      queue = queue_map[address];
    }

    if (queue != null) {

      // Schedule on the timer queue,
      timer.schedule(new TimerTask() {
        public void run() {
          ArrayList<String> types;
          ArrayList<MessageStream> messages;
          synchronized (queue_map) {
            // Remove from the queue,
            RetryMessageQueue queue = queue_map.remove(address);
            types = queue.service_types;
            messages = queue.queue;
          }
          // Create a message queue
          ServiceMessageQueue message_queue = createServiceMessageQueue();
          // For each message in the queue,
          int sz = types.size();
          for (int i = 0; i < sz; ++i) {
            String type = types.get(i);
            MessageStream message_stream = messages.get(i);

            if (type.equals("manager")) {
              sendManagerMessage(message_queue, address, message_stream);
            }
            else if (type.equals("root")) {
              sendRootMessage(message_queue, address, message_stream);
            }
            else {
              throw new RuntimeException("Unknown type");
            }
          }
          // Re-enqueue any pending messages,
          message_queue.enqueue();
        }
      }, 500);

    }
  }
		 */

		private RetryMessageQueue GetRetryMessageQueue(IServiceAddress service_addr) {
			lock (queueMap) {
				RetryMessageQueue queue;
				if (!queueMap.TryGetValue(service_addr, out queue)) {
					queue = new RetryMessageQueue(service_addr);
					queueMap[service_addr] = queue;
				}
				return queue;
			}
		}

		private int SendMessage(ServiceMessageQueue queue, IServiceAddress rootServer, ServiceType serviceType, Message request) {
			int successCount = 0;

			// Process the message,
			IMessageProcessor processor = connector.Connect(rootServer, serviceType);
			Message response = processor.Process(request);

			// Handle the response,
			if (response is MessageStream) {
				MessageStream messageStream = response as MessageStream;
				foreach (Message m in messageStream) {
					if (m.HasError) {
						log.Error("Root error: " + m.ErrorMessage);

						// If we failed, add the message to the retry queue,
						if (queue != null)
							queue.AddMessage(rootServer, serviceType, request);

						// TODO: We should confirm the error is a connection failure before
						//   reporting the service as down.
						// Report the service as down to the tracker,
						tracker.ReportServiceDownClientReport(rootServer, serviceType);
					} else {
						// Message successfully sent,
						++successCount;
					}
				}
			} else {
				if (response.HasError) {
					log.Error("Root error: " + response.ErrorMessage);

					// If we failed, add the message to the retry queue,
					if (queue != null)
						queue.AddMessage(rootServer, serviceType, request);

					// TODO: We should confirm the error is a connection failure before
					//   reporting the service as down.
					// Report the service as down to the tracker,
					tracker.ReportServiceDownClientReport(rootServer, serviceType);
				} else {
					successCount++;
				}
			}

			return successCount;
		}

		internal ServiceMessageQueue CreateServiceMessageQueue() {
			return new MCServiceMessageQueue(this);
		}

		#region RetryMessageQueue

		private class RetryMessageQueue {
			private readonly IServiceAddress serviceAddress;
			public readonly List<Message> queue;
			public readonly List<ServiceType> serviceTypes;

			internal RetryMessageQueue(IServiceAddress serviceAddress) {
				this.serviceAddress = serviceAddress;
				queue = new List<Message>();
				serviceTypes = new List<ServiceType>();
			}

			public void Add(Message message, ServiceType type) {
				lock (queue) {
					queue.Add(message);
					serviceTypes.Add(type);
					if (log.IsInterestedIn(LogLevel.Information)) {
						log.Info(String.Format("Queuing message {0} to service {1} {2}",
						                       new Object[] {message.ToString(), serviceAddress.ToString(), type}));
					}
				}
			}
		}

		#endregion

		#region MCServiceMessageQueue

		private class MCServiceMessageQueue : ServiceMessageQueue {
			private readonly MessageCommunicator communicator;

			public MCServiceMessageQueue(MessageCommunicator communicator) {
				this.communicator = communicator;
			}

			public override void Enqueue() {
				int sz = Messages.Count;
				for (int i = 0; i < sz; ++i) {
					EnqueuedMessage message = Messages[i];
					communicator.GetRetryMessageQueue(message.ServiceAddress).Add(message.Message, message.ServiceType);
				}
			}
		}

		#endregion
	}
}