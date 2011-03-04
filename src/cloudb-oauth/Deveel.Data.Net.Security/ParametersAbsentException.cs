using System;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public sealed class ParametersAbsentException : OAuthRequestException {
		private readonly string[] parameters;

		public ParametersAbsentException(string message, string[] parameters)
			: base(message, OAuthProblemTypes.ParameterAbsent) {
			this.parameters = parameters;

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < parameters.Length; i++) {
				sb.Append(Rfc3986.Encode(parameters[i]));

				if (i < parameters.Length - 1)
					sb.Append("&");
			}

			AddParameter(OAuthErrorParameterKeys.ParametersAbsent, sb.ToString());
		}

		public ParametersAbsentException(string message, string parameter)
			: this(message, new string[] { parameter }) {
		}

		public string[] MissingParameters {
			get { return parameters; }
		}
	}
}