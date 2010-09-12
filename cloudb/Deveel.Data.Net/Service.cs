using System;
using System.ComponentModel;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public abstract class Service : Component, IService {
		private IMessageProcessor processor;
		private Logger log;
		private ErrorStateException errorState;

		private bool disposed;
		private bool initialized;

		protected Service() {
			log = LogManager.GetLogger("network", GetType());
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
		}

		protected override void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					log.Dispose();
					log = null;
				}

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