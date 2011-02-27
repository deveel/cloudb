using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Net.Security {
	public interface IUserDataSource {
		void Init(ConfigSource config);

		IUser FindUser(string name);
	}
}