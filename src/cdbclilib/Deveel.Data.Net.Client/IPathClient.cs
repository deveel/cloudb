using System;

namespace Deveel.Data.Net.Client {
	public interface IPathClient : IDisposable {
		string PathName { get; }

		ClientState State { get; }


		void Open();

		void Close();

		IPathTransaction BeginTransaction();
	}
}