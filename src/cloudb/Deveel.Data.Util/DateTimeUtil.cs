using System;

namespace Deveel.Data.Util {
	static class DateTimeUtil {
		private static readonly DateTime UnixEpoc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		public static long CurrentTimeMillis() {
			return (long) DateTime.UtcNow.Subtract(UnixEpoc).TotalMilliseconds;
		}

		public static long GetMillis(DateTime time) {
			return (long) time.ToUniversalTime().Subtract(UnixEpoc).TotalMilliseconds;
		}
	}
}