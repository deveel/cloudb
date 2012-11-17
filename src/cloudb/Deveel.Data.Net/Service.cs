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
using System.ComponentModel;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Messaging;

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

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Stop();
			}

			base.Dispose(disposing);
		}
	}
}