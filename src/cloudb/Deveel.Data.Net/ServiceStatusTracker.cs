using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;

namespace Deveel.Data.Net {
	public sealed class ServiceStatusTracker {
		private readonly HeartbeatThread heartbeatThread;
		private readonly List<TrackedService> monitoredServers;

		private readonly Logger log = Logger.Network;

		public ServiceStatusTracker(IServiceConnector connector) {
			monitoredServers = new List<TrackedService>(128);
			heartbeatThread = new HeartbeatThread(this, connector, monitoredServers);
		}

		public event ServiceStatusEventHandler StatusChange;

		public void Stop() {
			heartbeatThread.Finish();
		}

		public ServiceStatus GetServiceCurrentStatus(IServiceAddress address, ServiceType type) {
			// Search and return,
			// TODO: Should this be a hash lookup instead for speed?
			lock (monitoredServers) {
				foreach (TrackedService s in monitoredServers) {
					if (s.ServiceAddress.Equals(address) &&
					    s.ServiceType == type) {
						return s.CurrentStatus;
					}
				}
			}
			// Not found in list, so assume the service is up,
			return ServiceStatus.Up;
		}

		public bool IsServiceUp(IServiceAddress address, ServiceType type) {
			return GetServiceCurrentStatus(address, type) == ServiceStatus.Up;
		}

		private void ReportServiceDown(IServiceAddress address, ServiceType type, ServiceStatus status) {
			// Default old status,
			ServiceStatus oldStatus = ServiceStatus.Up;

			// Search and return,
			lock (monitoredServers) {
				TrackedService tracked = null;
				foreach (TrackedService s in monitoredServers) {
					if (s.ServiceAddress.Equals(address) &&
					    s.ServiceType == type) {
						tracked = s;
						break;
					}
				}

				if (tracked == null) {
					// Not found so add it to the tracker,
					monitoredServers.Add(new TrackedService(address, type, status));
				} else {
					oldStatus = tracked.CurrentStatus;
					tracked.CurrentStatus = status;
				}
			}

			// Fire the event if the status changed,
			if (!oldStatus.Equals(status)) {
				if (StatusChange != null)
					StatusChange(this, new ServiceStatusEventArgs(address, type, oldStatus, status));
			}
		}

		public void ReportServiceDownClientReport(IServiceAddress address, ServiceType type) {
			if (log.IsInterestedIn(LogLevel.Information))
				log.Info(string.Format("reportServiceDownClientReport {0} {1}", address, type));

			ReportServiceDown(address, type, ServiceStatus.DownClientReport);
		}

		public void ReportServiceDownShutdown(IServiceAddress address, ServiceType type) {
			if (log.IsInterestedIn(LogLevel.Information))
				log.Info(String.Format("reportServiceDownShutdown {0} {1}", address, type));

			ReportServiceDown(address, type, ServiceStatus.DownShutdown);
		}


		#region HeartbeatThread

		private class HeartbeatThread {
			private readonly ServiceStatusTracker tracker;
			private readonly Thread thread;

			private const int PollDelayMs = (10*1000);

			private bool finished;

			private readonly IServiceConnector connector;
			private readonly List<TrackedService> monitoredServers;


			public HeartbeatThread(ServiceStatusTracker tracker, IServiceConnector connector,
			                       List<TrackedService> monitoredServers) {
				this.tracker = tracker;
				this.connector = connector;
				this.monitoredServers = monitoredServers;

				thread = new Thread(Execute);
				thread.IsBackground = true;
				thread.Name = "StatusTracker::Heatbeat";
				thread.Start();
			}

			private void PollServer(TrackedService server) {
				bool pollOk = true;

				string commandArg = null;
				if (server.ServiceType == ServiceType.Block)
					commandArg = "heartbeatB";
				else if (server.ServiceType == ServiceType.Root)
					commandArg = "heartbeatR";
				else if (server.ServiceType == ServiceType.Manager)
					commandArg = "heartbeatM";
				else {
					tracker.log.Error(String.Format("Don't know how to poll type {0}", server.ServiceType));
					pollOk = false;
				}

				// Send the poll command to the server,
				IMessageProcessor p = connector.Connect(server.ServiceAddress, ServiceType.Block);
				MessageStream outputStream = new MessageStream();
				outputStream.AddMessage(new Message("poll", commandArg));
				IEnumerable<Message> inputStream = p.Process(outputStream);
				foreach (Message m in inputStream) {
					// Any error with the poll means no status change,
					if (m.HasError) {
						pollOk = false;
					}
				}

				// If the poll is ok, set the status of the server to UP and remove from
				// the monitor list,
				if (pollOk) {
					// The server status is set to 'STATUS_UP' if either the current state
					// is 'DOWN CLIENT REPORT' or 'DOWN HEARTBEAT'
					// Synchronize over 'servers_map' for safe alteration of the ref.
					ServiceStatus oldStatus;
					lock (monitoredServers) {
						oldStatus = server.CurrentStatus;
						if (oldStatus == ServiceStatus.DownClientReport ||
						    oldStatus == ServiceStatus.DownHeartbeat) {
							server.CurrentStatus = ServiceStatus.Up;
						}
						// Remove the server from the monitored_servers list.
						monitoredServers.Remove(server);
					}

					if (tracker.log.IsInterestedIn(LogLevel.Information)) {
						tracker.log.Info(String.Format("Poll ok. Status now UP for {0} {1}", server.ServiceAddress, server.ServiceType));
					}

					// Fire the event if the status changed,
					try {
						if (tracker.StatusChange != null)
							tracker.StatusChange(this,
							                     new ServiceStatusEventArgs(server.ServiceAddress, server.ServiceType, oldStatus,
							                                                ServiceStatus.Up));
					} catch (Exception e) {
						// Catch any exception generated. Log it but don't terminate the
						// thread.
						tracker.log.Error("Exception in listener during poll", e);
					}

				} else {
					// Make sure the server status is set to 'DOWN HEARTBEAT' if the poll
					// failed,
					// Synchronize over 'servers_map' for safe alteration of the ref.
					lock (monitoredServers) {
						ServiceStatus sts = server.CurrentStatus;
						if (sts == ServiceStatus.Up ||
						    sts == ServiceStatus.DownClientReport) {
							server.CurrentStatus = ServiceStatus.DownHeartbeat;
						}
					}
				}
			}


			private void Execute() {
				try {
					while (true) {
						List<TrackedService> servers;
						// Wait on the poll delay
						lock (this) {
							if (finished) {
								return;
							}
							Monitor.Wait(this, PollDelayMs);
							if (finished) {
								return;
							}
						}
						lock (monitoredServers) {
							// If there are no servers to monitor, continue the loop,
							if (monitoredServers.Count == 0) {
								continue;
							}
							// Otherwise, copy the monitored servers into the 'servers'
							// object,
							servers = new List<TrackedService>(monitoredServers.Count);
							servers.AddRange(monitoredServers);
						}
						// Poll the servers
						foreach (TrackedService s in servers) {
							PollServer(s);
						}
					}
				} catch (ThreadInterruptedException e) {
					tracker.log.Warning("Heartbeat thread interrupted");
				}

			}

			public void Finish() {
				lock (this) {
					finished = true;
					Monitor.PulseAll(this);
				}
			}
		}

		#endregion

		#region TrackedService

		private class TrackedService {
			private readonly IServiceAddress serviceAddress;
			private readonly ServiceType serviceType;
			private ServiceStatus currentStatus;

			public TrackedService(IServiceAddress serviceAddress, ServiceType serviceType, ServiceStatus currentStatus) {
				this.serviceAddress = serviceAddress;
				this.serviceType = serviceType;
				this.currentStatus = currentStatus;
			}

			public ServiceStatus CurrentStatus {
				get { return currentStatus; }
				set { currentStatus = value; }
			}

			public ServiceType ServiceType {
				get { return serviceType; }
			}

			public IServiceAddress ServiceAddress {
				get { return serviceAddress; }
			}

			public override int GetHashCode() {
				return serviceAddress.GetHashCode() + serviceType.GetHashCode();
			}

			public override bool Equals(object obj) {
				TrackedService other = obj as TrackedService;
				if (other == null)
					return false;

				if (serviceAddress != other.serviceAddress &&
				    (serviceAddress == null ||
				     !serviceAddress.Equals(other.serviceAddress)))
					return false;

				if (!serviceType.Equals(other.serviceType))
					return false;

				return true;
			}
		}

		#endregion
	}
}