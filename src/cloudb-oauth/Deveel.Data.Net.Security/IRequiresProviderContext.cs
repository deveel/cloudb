using System;

namespace Deveel.Data.Net.Security {
	public interface IRequiresProviderContext {
		IOAuthProvider Context { get; set; }
	}
}