using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public interface IOAuthError {
		string Problem { get; }

		string Advice { get; }

		IDictionary<string, string> Parameters { get; }
	}
}