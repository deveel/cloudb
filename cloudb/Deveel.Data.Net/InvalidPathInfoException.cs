using System;

namespace Deveel.Data.Net {
	public class InvalidPathInfoException : ApplicationException {
		internal InvalidPathInfoException(string message)
			: base(message) {
		}
	}
}