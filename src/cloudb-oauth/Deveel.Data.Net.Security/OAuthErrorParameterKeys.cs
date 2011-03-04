using System;

namespace Deveel.Data.Net.Security {
	public static class OAuthErrorParameterKeys {
		public const string Problem = "oauth_problem";
		public const string AcceptableVersions = "oauth_acceptable_versions";
		public const string AcceptableTimestamps = "oauth_acceptable_timestamps";
		public const string ParametersAbsent = "oauth_parameters_absent";
		public const string ParametersRejected = "oauth_parameters_rejected";
		public const string ProblemAdvice = "oauth_problem_advice";
	}
}