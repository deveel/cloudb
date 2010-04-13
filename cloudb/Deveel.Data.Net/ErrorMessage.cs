using System;

namespace Deveel.Data.Net {
	public class ErrorMessage : Message {
		internal ErrorMessage(ServiceException error)
			: base("E", new object[] { error }) {
		}

		internal ErrorMessage()
			: this(null) {
		}

		public ServiceException Error {
			get { return (ServiceException) this[0]; }
		}
	}
}