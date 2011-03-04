using System;
using System.Globalization;

namespace Deveel.Data.Net.Security {
	public sealed class TimestampRefusedException : OAuthRequestException {
		private readonly long minTimestamp;
		private readonly long maxTimestamp;

		public TimestampRefusedException(string message, long minTimestamp, long maxTimestamp)
			: base(message, OAuthProblemTypes.TimestampRefused) {
			this.minTimestamp = minTimestamp;
			this.maxTimestamp = maxTimestamp;

			AddParameter(OAuthErrorParameterKeys.AcceptableTimestamps,
			             String.Format(CultureInfo.InvariantCulture, "{0}-{1}", minTimestamp, maxTimestamp));
		}

		public long MaxTimestamp {
			get { return maxTimestamp; }
		}

		public long MinTimestamp {
			get { return minTimestamp; }
		}
	}
}