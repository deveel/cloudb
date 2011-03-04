using System;

namespace Deveel.Data.Net.Security {
	public interface IUser {
		string Name { get; }


		bool ValidatePassword(string password);
	}
}