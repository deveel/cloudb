using System;

namespace Deveel.Data.Net.Security {
	public interface IRequestStateStore {
		void Store(RequestState state);

		RequestState Get(RequestStateKey key);

		void Delete(RequestStateKey key);
	}
}