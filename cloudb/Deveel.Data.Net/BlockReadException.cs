using System;

namespace Deveel.Data.Net {
	public class BlockReadException : ApplicationException {
		public BlockReadException(string message)
			: base(message) {
		}
		
		public BlockReadException(string message, Exception innerException)
			: base(message, innerException) {
		}
	}
}