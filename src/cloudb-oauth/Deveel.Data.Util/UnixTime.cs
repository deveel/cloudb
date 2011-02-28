using System;

namespace Deveel.Data.Net.Security {
	static class UnixTime {
		private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, 0);

		public static int ToUnixTime(DateTime time) {
			return (int)(time.ToUniversalTime() - UnixEpoch).TotalSeconds;
		}

		public static DateTime FromUnixTime(long unixTime) {
			if (unixTime < 0)
				throw new ArgumentOutOfRangeException("unixTime");

			return UnixEpoch.AddSeconds(unixTime);
		}
	}
}