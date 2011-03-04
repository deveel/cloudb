using System;

namespace Deveel.Data.Net.Security {
	public sealed class VersionRejectedException : OAuthRequestException {
		private readonly string minVersion;
		private readonly string maxVersion;

		public VersionRejectedException(string message, string minVersion, string maxVersion) 
			: base(message, OAuthProblemTypes.VersionRejected) {
			if (String.IsNullOrEmpty(minVersion))
				throw new ArgumentNullException("minVersion");
			if (String.IsNullOrEmpty(maxVersion))
				throw new ArgumentNullException("maxVersion");

			this.minVersion = minVersion;
			this.maxVersion = maxVersion;

			AddParameter(OAuthErrorParameterKeys.AcceptableVersions, minVersion + '-' + maxVersion);
		}

		public string MaxVersion {
			get { return maxVersion; }
		}

		public string MinVersion {
			get { return minVersion; }
		}
	}
}