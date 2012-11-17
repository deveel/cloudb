//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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
					DateTime clearBefore = timestampRecorded - purgeTimeframe;
					LinkedListNode<AnalyticsRecord> node = history.First;
					while (node != null) {
						AnalyticsRecord arr = node.Value;
						if (arr.End < clearBefore)
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