using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthParameters {
		private readonly IDictionary<string, string> parameters;

		internal OAuthParameters() {
			parameters = new Dictionary<string, string>();
		}

		public string GetValue(string key) {
			string value;
			if (parameters.TryGetValue(key, out value))
				return value;
			return null;
		}
	}
}