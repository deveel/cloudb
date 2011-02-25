using System;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class ServiceStatusTracker {
		private readonly List<TrackedService> monitored_servers;
		private readonly IServiceConnector serviceConnector;
		private readonly Thread heartbeatThread;
		//TODO: make this a configurable variable ...
		private int pollDelay = (10 * 1000);
		private bool finished;

		private readonly static Logger log = Logger.Network;

		public event ServiceStatusEventHandler StatusChange;


		public ServiceStatusTracker(IServiceConnector network) {
			serviceConnector = network;
			monitored_servers = new List<TrackedService>(128);

			// Start the tracker,
			heartbeatThread = new Thread(HeartBeat);
			heartbeatThread.IsBackground = true;
			heartbeatThread.Start();
		}

		private void OnStatusChange(IServiceAddress address, ServiceType serviceType, ServiceStatus oldStatus, ServiceStatus newStatus) {
			if (StatusChange != null)
				StatusChange(this, new ServiceStatusEventArgs(address, serviceType, oldStatus, newStatus));
		}

		private void ReportServiceDown(IServiceAddress address, ServiceType type, ServiceStatus status) {
			// Default old status,
			ServiceStatus oldStatus = ServiceStatus.Up;

			// Search and return,
			lock (monitored_servers) {
				TrackedService tracked = null;
				foreach (TrackedService s in monitored_servers) {
					if (s.Address.Equals(address) &&
					    s.Type == type) {
						tracked = s;
						break;
					}
				}

				if (tracked == null) {
					// Not found so add it to the tracker,
					monitored_servers.Add(new TrackedService(address, type, status));
				} else {
					oldStatus = tracked.CurrentStatus;
					tracked.CurrentStatus = status;
				}
			}

			// Fire the event if the status changed,
			if (oldStatus != status)
				OnStatusChange(address, type, oldStatus, status);
		}

		private void HeartBeat() {
			try {
				while (true) {
					List<TrackedService> servers;
					// Wait on the poll delay
					lock (this) {
						if (finished) {
							return;
						}
						Monitor.Wait(this, pollDelay);
						if (finished) {
							return;
						}
					}
					lock (monitored_servers) {
						// If there are no servers to monitor, continue the loop,
						if (monitored_servers.Count == 0) {
							continue;
						}
						// Otherwise, copy the monitored servers into the 'servers'
						// object,
						servers = new List<TrackedService>(monitored_servers.Count);
						servers.AddRange(monitored_servers);
					}
					// Poll the servers
					foreach (TrackedService s in servers) {
						PollService(s);
					}
				}
			} catch (ThreadInterruptedException e) {
				log.Warning("Heartbeat thread interrupted");
			}
		}

		private void PollService(TrackedService service) {
			bool pollOk = true;

			if (service.Type == ServiceType.Admin) {
				log.Error("Don't know how to poll type Admin");
				pollOk = false;
			} else {
				// Send the poll command to the server,
				IMessageProcessor p = serviceConnector.Connect(service.Address, service.Type);
				RequestMessage request = new RequestMessage("poll");
				if (service.Type == ServiceType.Block)
					request.Arguments.Add("heatbeatB");
				else if (service.Type == ServiceType.Manager)
					request.Arguments.Add("heatbeatM");
				else if (service.Type == ServiceType.Root)
					request.Arguments.Add("heatbeatR");
				Message response = p.Process(request);
				if (response.HasError)
					// Any error with the poll means no status change,
					pollOk = false;
			}

			// If the poll is ok, set the status of the server to UP and remove from
			// the monitor list,
			if (pollOk) {
				// The server status is set to 'UP' if either the current state
				// is 'DOWN CLIENT REPORT' or 'DOWN HEARTBEAT'
				// Synchronize over 'servers_map' for safe alteration of the ref.
				ServiceStatus oldStatus;
				lock (monitored_servers) {
					oldStatus = service.CurrentStatus;
					if (oldStatus == ServiceStatus.DownClientReport ||
					    oldStatus == ServiceStatus.DownHeartbeat) {
						service.CurrentStatus = ServiceStatus.Up;
					}
					// Remove the server from the monitored_servers list.
					monitored_servers.Remove(service);
				}

				if (log.IsInterestedIn(LogLevel.Information))
					log.Info(String.Format("Poll ok. Status now UP for {0} {1}", new Object[] { service.Address.ToString(), service.Type }));

				// Fire the event if the status changed,
				try {
					OnStatusChange(service.Address, service.Type, oldStatus, ServiceStatus.Up);
				} catch (Exception e) {
					// Catch any exception generated. Log it but don't terminate the
					// thread.
					log.Error("Exception in event during poll", e);
				}
			} else {
				// Make sure the server status is set to 'DOWN HEARTBEAT' if the poll
				// failed,
				// Synchronize over 'servers_map' for safe alteration of the ref.
				lock (monitored_servers) {
					ServiceStatus sts = service.CurrentStatus;
					if (sts == ServiceStatus.Up ||
					    sts == ServiceStatus.DownClientReport) {
						service.CurrentStatus = ServiceStatus.DownHeartbeat;
					}
				}
			}
		}

		private void FinishHeartBeat() {
			lock (this) {
				finished = true;
				Monitor.PulseAll(this);
			}
		}

		public void Stop() {
			FinishHeartBeat();
		}

		public ServiceStatus GetServiceCurrentStatus(IServiceAddress address, ServiceType type) {
			// Search and return,
			// TODO: Should this be a hash lookup instead for speed?
			lock (monitored_servers) {
				foreach (TrackedService s in monitored_servers) {
					if (s.Address.Equals(address) &&
						s.Type == type) {
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

		public void ReportServiceDownClientReport(IServiceAddress address, ServiceType type) {
			if (log.IsInterestedIn(LogLevel.Information))
				log.Info(String.Format("reportServiceDownClientReport {0} {1}", new Object[] { address.ToString(), type }));

			ReportServiceDown(address, type, ServiceStatus.DownClientReport);
		}

		public void ReportServiceDownShutdown(IServiceAddress address, ServiceType type) {
			if (log.IsInterestedIn(LogLevel.Information))
				log.Info(String.Format("reportServiceDownShutdown {0} {1}", new Object[] { address.ToString(), type }));

			ReportServiceDown(address, type, ServiceStatus.DownShutdown);
		}

		#region TrackedService

		private class TrackedService {

			// The network address of the service,
			public readonly IServiceAddress Address;
			public readonly ServiceType Type;
			// The current status
			public volatile ServiceStatus CurrentStatus;

			public TrackedService(IServiceAddress address, ServiceType type, ServiceStatus status) {
				Address = address;
				Type = type;
				CurrentStatus = status;
			}

			public TrackedService(IServiceAddress address, ServiceType type)
				: this(address, type, ServiceStatus.Up) {
			}

			public override int GetHashCode() {
				return Address.GetHashCode() + Type.GetHashCode();
			}

			public override bool Equals(Object obj) {
				if (this == obj)
					return true;
				TrackedService other = obj as TrackedService;
				if (other == null)
					return false;

				if (Address != other.Address &&
					(Address == null || !Address.Equals(other.Address)))
					return false;
				if (Type != other.Type)
					return false;

				return true;
			}

		}

		#endregion
	}
}