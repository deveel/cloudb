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

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Represents an event record in the <see cref="Analytics"/> history.
	/// </summary>
	public sealed class AnalyticsRecord {
		internal AnalyticsRecord(DateTime start, DateTime end, TimeSpan timeInOps, long ops) {
			this.start = start;
			this.end = end;
			this.timeInOps = timeInOps;
			this.ops = ops;
		}

		internal AnalyticsRecord(long start, long end, long timeInOps, long ops)
			: this(DateTime.FromBinary(start), DateTime.FromBinary(end), new TimeSpan(timeInOps * TimeSpan.TicksPerMillisecond), ops) {
		}

		private readonly DateTime start;
		private readonly DateTime end;
		private readonly TimeSpan timeInOps;
		private readonly long ops;

		/// <summary>
		/// The end time of the event.
		/// </summary>
		public DateTime End {
			get { return end; }
		}

		/// <summary>
		/// The start time of the event.
		/// </summary>
		public DateTime Start {
			get { return start; }
		}

		/// <summary>
		/// Gets the number of operations computed during the event time.
		/// </summary>
		public long Operations {
			get { return ops; }
		}

		/// <summary>
		/// Gets the time spent for all the operations within the
		/// time frame of the event.
		/// </summary>
		public TimeSpan TotalOperationsTime {
			get { return timeInOps; }
		}

		internal long [] ToArray() {
			long[] array = new long[4];
			array[0] = start.ToBinary();
			array[1] = end.ToBinary();
			array[2] = timeInOps.Ticks;
			array[3] = ops;
			return array;
		}
	}
}