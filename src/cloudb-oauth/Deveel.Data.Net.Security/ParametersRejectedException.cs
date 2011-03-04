using System;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public sealed class ParametersRejectedException : OAuthRequestException {
		private readonly string[] parameters;

		public ParametersRejectedException(string message, string[] parameters)
			: base(message, OAuthProblemTypes.ParameterRejected) {
			this.parameters = parameters;

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < parameters.Length; i++) {
				sb.Append(Rfc3986.Encode(parameters[i]));

				if (i < parameters.Length - 1)
					sb.Append("&");
			}

			AddParameter(OAuthErrorParameterKeys.ParametersRejected, sb.ToString());
		}

		public string[] RejectedParameters {
			get { return parameters; }
		}
	}
}