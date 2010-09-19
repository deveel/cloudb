using System;

namespace Deveel.Json {
	class JSONException : ApplicationException {
		public JSONException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public JSONException(string message)
			: base(message) {
		}

		public JSONException() {
		}
	}
}