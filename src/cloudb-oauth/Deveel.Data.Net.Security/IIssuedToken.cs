using System;

namespace Deveel.Data.Net.Security {
	public interface IIssuedToken : IToken {
		TokenStatus Status { get; }

		void ChangeStatus(TokenStatus newStatus);
	}
}