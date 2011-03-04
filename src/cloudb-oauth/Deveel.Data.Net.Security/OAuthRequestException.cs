using System;
using System.Collections.Generic;

namespace Deveel.Data.Net.Security {
	public class OAuthRequestException : AuthenticationException, IOAuthError {
		private readonly string problemType;
		private readonly IDictionary<string, string> parameters;

		public OAuthRequestException(string message, string problemType)
			: base(message, ErrorCodes.GetCode(problemType)) {
			this.problemType = problemType;
			parameters = new Dictionary<string, string>();
		}

		public OAuthRequestException(string message)
			: this(message, null) {
		}

		public string Problem {
			get { return problemType; }
		}

		string IOAuthError.Advice {
			get { return Message; }
		}

		IDictionary<string,string> IOAuthError.Parameters {
			get { return parameters; }
		}

		protected void AddParameter(string key, string value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			parameters.Add(key, value);
		}

		public static void TryRethrow(OAuthParameters parameters) {
			if (!parameters.HasProblem) {
				OAuthRequestException ex = new OAuthRequestException(parameters.ProblemAdvice, parameters.ProblemType);

				// Load additional parameter for specific types
				switch (parameters.ProblemType) {
					case OAuthProblemTypes.VersionRejected:
						if (!String.IsNullOrEmpty(parameters.AcceptableVersions))
							ex.AddParameter(OAuthErrorParameterKeys.AcceptableVersions, parameters.AcceptableVersions);
						break;
					case OAuthProblemTypes.ParameterAbsent:
						if (!String.IsNullOrEmpty(parameters.ParametersAbsent))
							ex.AddParameter(OAuthErrorParameterKeys.ParametersAbsent, parameters.ParametersAbsent);
						break;
					case OAuthProblemTypes.ParameterRejected:
						if (!String.IsNullOrEmpty(parameters.ParametersRejected))
							ex.AddParameter(OAuthErrorParameterKeys.ParametersRejected, parameters.ParametersRejected);
						break;
					case OAuthProblemTypes.TimestampRefused:
						if (!String.IsNullOrEmpty(parameters.AcceptableTimestamps))
							ex.AddParameter(OAuthErrorParameterKeys.AcceptableTimestamps, parameters.AcceptableTimestamps);
						break;
				}

				// Throw the OAuthRequestException
				throw ex;
			}
		}
	}
}