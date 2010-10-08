using System;
using System.ComponentModel;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public abstract class Service : Component, IService {
		private IMessageProcessor processor;
		private Logger log;
		private ErrorStateException errorState;

		private bool disposed;
		private bool initialized;

		protected Service() {
			log = LogManager.NetworkLogger;
		}

		~Service() {
			Dispose(false);
		}

		protected Logger Logger {
			get { return log; }
		}

		public abstract ServiceType ServiceType { get; }

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
		}

		protected abstract IMessageProcessor CreateProcessor();

		protected virtual void OnDispose(bool disposing) {
			if (disposing) {
//				log.Dispose();
//				log = null;
			}
		}

		protected override void Dispose(bool disposing) {
			if (!disposed) {
				OnDispose(disposing);

				disposed = true;
			}
		}

		protected virtual void OnInit() {
		}

		public void Init() {
			if (initialized)
				throw new InvalidOperationException("The service is already initialized.");

			OnInit();

			initialized = true;
		}
	}
}