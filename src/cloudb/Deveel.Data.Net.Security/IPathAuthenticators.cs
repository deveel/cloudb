using System;

namespace Deveel.Data.Net.Security {
	public interface IPathAuthenticators {
		IAuthenticator[] GetAuthenticators(string pathName);

		bool AddAuthenticator(string pathName, string mechanism);

		bool RemoveAuthenticator(string pathName, string mechanism);
	}
}