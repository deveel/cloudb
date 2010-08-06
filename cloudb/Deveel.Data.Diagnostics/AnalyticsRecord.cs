using System;

namespace Deveel.Data.Diagnostics {
	public sealed class AnalyticsRecord {
		internal AnalyticsRecord(DateTime start, DateTime end, TimeSpan timeInOps, long ops) {
			this.start = start;
			this.end = end;
			this.timeInOps = timeInOps;
			this.ops = ops;
		}

		internal AnalyticsRecord(long start, long end, long timeInOps, long ops)
			: this(new DateTime(start), new DateTime(end), new TimeSpan(timeInOps * TimeSpan.TicksPerMillisecond), ops) {
		}

		private readonly DateTime start;
		private readonly DateTime end;
		private readonly TimeSpan timeInOps;
		private readonly long ops;

		public DateTime End {
			get { return end; }
		}

		public DateTime Start {
			get { return start; }
		}

		public long Operations {
			get { return ops; }
		}

		public TimeSpan TotalOperationsTime {
			get { return timeInOps; }
		}
	}
}