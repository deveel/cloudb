using System;

namespace Deveel.Data.Net.Security {
	public interface ICallbackStore {
		bool SaveCallback(IRequestToken token, Uri callback);

		bool ObtainCallback(IRequestToken token, out Uri callback);
	}
}