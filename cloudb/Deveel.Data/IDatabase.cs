using System;

namespace Deveel.Data {
	public interface IDatabase {
		ITransaction CreateTransaction();

		void Publish(ITransaction transaction);

		void Dispose(ITransaction transaction);

		void CheckPoint();
	}
}