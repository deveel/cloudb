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

		protected Service() {
			log = LogManager.NetworkLogger;
		}

		protected Logger Logger {
			get { return log; }
		}

		public abstract ServiceType ServiceType { get; }

		public ServiceState State {
			get { return state; }
		}

		public IMessageProcessor Processor {
			get {
				if (processor == null)
					processor = CreateProcessor();
				return processor;
			}
		}

		protected void CheckErrorState() {
			if (errorState != null)
				throw errorState;
		}

		protected void SetErrorState(Exception e) {
			errorState = new ErrorStateException(e);
			state = ServiceState.Error;
		}

		protected abstract IMessageProcessor CreateProcessor();

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if(state != ServiceState.Stopped)
					Stop();
			}

			base.Dispose(disposing);
		}

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
				} catch(Exception e) {
					Logger.Error(e);
					SetErrorState(e);
				}
			}
		}
	}
}