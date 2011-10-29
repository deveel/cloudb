using System;

using Deveel.Data.Net.Security;

namespace Deveel.Data.Net {
	public interface IConnection : IDisposable {
		IServiceAddress Address { get; }
		
		bool IsOpened { get; }

		bool IsAuthenticated { get; }
		
		
		void Open();

		AuthResponse Authenticate(AuthRequest request);

		bool EndAuthenticatedSession(object context);
		
		void Close();
	}
}

