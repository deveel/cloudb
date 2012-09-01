using System;

namespace Deveel.Data.Net {
	public class InvalidPathInfoException : ApplicationException {
		public InvalidPathInfoException(string message)
			: base(message) {
		}
	}
}