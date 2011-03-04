using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public sealed class Analytics {
		public Analytics(TimeSpan timeframeSize, TimeSpan purgeTimeframe) {
			this.timeframeSize = timeframeSize;
			this.purgeTimeframe = purgeTimeframe;
		}

		public Analytics()
			: this(new TimeSpan(1 * TimeSpan.TicksPerHour), new TimeSpan(1 * TimeSpan.TicksPerDay)) {
		}

		private readonly LinkedList<AnalyticsRecord> history = new LinkedList<AnalyticsRecord>();
		private long opsCount;
		private TimeSpan timeInOps;
		private DateTime timeframeStart;
		private readonly TimeSpan timeframeSize;
		private readonly TimeSpan purgeTimeframe;

		public void AddEvent(DateTime timestampRecorded, TimeSpan timeSpent) {
			lock (this) {
				timeInOps += timeSpent;
				opsCount += 1;
				// Go to the next timeframe?
				if ((timestampRecorded - timeframeStart) > timeframeSize) {
					// Record the timeframe in the history
					history.AddLast(new AnalyticsRecord(timeframeStart, timestampRecorded, timeInOps, opsCount));
					// Should we clear early events?
					DateTime clear_before = timestampRecorded - purgeTimeframe;
					LinkedListNode<AnalyticsRecord> node = history.First;
					while (node != null) {
						AnalyticsRecord arr = node.Value;
						if (arr.End < clear_before)
							history.Remove(node);
						else
							break;

						node = node.Next;
					}
					// Reset the current timeframe stats
					timeframeStart = timestampRecorded;
					timeInOps = TimeSpan.Zero;
					opsCount = 0;
				}
			}
		}

		public AnalyticsRecord[] GetStats() {
			// Copy the history,
			lock (this) {
				int sz = history.Count;
				List<AnalyticsRecord> output = new List<AnalyticsRecord>(sz);
				foreach (AnalyticsRecord v in history) {
					output.Add(v);
				}
				return output.ToArray();
			}
		}
	}
}