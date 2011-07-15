using System;
using System.ComponentModel;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public abstract class Service : Component, IService {
		private IMessageProcessor processor;
		private readonly Logger log;
		private ErrorStateException errorState;
		private ServiceState state;

		public event EventHandler Started;
		public event EventHandler Stopped;
		public event EventHandler Error;

		protected Service() {
			log = Logger.Network;
		}

		protected Logger Logger {
			get { return log; }
		}

		public abstract ServiceType ServiceType { get; }

		public ServiceState State {
			get { return state; }
		}

		public IMessageProcessor Processor {
			get { return processor ?? (processor = CreateProcessor()); }
		}

		protected void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}

		protected void SetErrorState(Exception e) {
			errorState = new ErrorStateException(e);
			state = ServiceState.Error;

			if (Error != null)
				Error(this, EventArgs.Empty);
		}

		protected abstract IMessageProcessor CreateProcessor();

		protected virtual void OnStart() {
		}

		protected virtual void OnStop() {
		}

		public void Start() {
			if (state == ServiceState.Started)
				throw new InvalidOperationException("The service is already initialized.");

			try {
				OnStart();
				state = ServiceState.Started;

				if (Started != null)
					Started(this, EventArgs.Empty);
			} catch(Exception e) {
				Logger.Error(e);
				SetErrorState(e);
				throw;
			}
		}

		public void Stop() {
			if (state != ServiceState.Stopped) {
				try {
					OnStop();
					state = ServiceState.Stopped;

					if (Stopped != null)
						Stopped(this, EventArgs.Empty);
				} catch(Exception e) {
					Logger.Error(e);
					SetErrorState(e);
					throw;
				}
			}
		}
	}
}