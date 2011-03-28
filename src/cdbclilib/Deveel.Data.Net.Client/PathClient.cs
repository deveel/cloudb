using System;

namespace Deveel.Data.Net.Client {
	public abstract class PathClient : IPathClient {
		private readonly string pathName;
		private ClientState state;

		protected PathClient(string pathName) {
			if (String.IsNullOrEmpty(pathName))
				throw new ArgumentNullException("pathName");

			this.pathName = pathName;
		}

		~PathClient() {
			Dispose(false);
		}

		public string PathName {
			get { return pathName; }
		}

		public event EventHandler Opened;
		public event EventHandler Closed;

		public ClientState State {
			get { return state; }
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (state != ClientState.Closed)
					Close();
			}
		}

		protected abstract void OpenConnection();

		protected abstract void CloseConnection();

		protected abstract IPathTransaction CreateTransaction();

		private void OnClosed() {
			if (Closed != null)
				Closed(this, EventArgs.Empty);
		}

		private void OnOpen() {
			if (Opened != null)
				Opened(this, EventArgs.Empty);
		}

		public void Close() {
			try {
				CloseConnection();
				OnClosed();
				state = ClientState.Closed;
			} catch (Exception) {
				state = ClientState.Broken;
				throw;
			}
		}

		public void Open() {
			try {
				OpenConnection();
				OnOpen();
				state = ClientState.Open;
			} catch (Exception) {
				state = ClientState.Broken;
				throw;
			}
		}

		IPathTransaction IPathClient.BeginTransaction() {
			return CreateTransaction();
		}

		void IDisposable.Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
