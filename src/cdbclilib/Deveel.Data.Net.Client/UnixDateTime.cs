using System;

namespace Deveel.Data.Net.Client {
	internal static class UnixDateTime {
		private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);

		public static DateTime ToDateTime(long unixTimestamp) {
			return UnixEpoch.AddMilliseconds(unixTimestamp);
		}

		public static long ToUnixTimestamp(DateTime dateTime) {
			return (long) (dateTime - UnixEpoch).TotalMilliseconds;
		}
	}
}