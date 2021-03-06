﻿//
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
using System.Text;

namespace Deveel {
	public class Quadruple : IComparable<Quadruple>, IComparable {
		private readonly long[] components;

		public Quadruple(long[] components) {
			this.components = components;
		}

		public Quadruple(long high, long low)
			: this(new long[] {high, low}) {
		}

		public long High {
			get { return components[0]; }
		}

		public long Low {
			get { return components[1]; }
		}

		public override int GetHashCode() {
			return (int) (components[1] & 0x07FFFFFFFL);
		}

		public override bool Equals(object obj) {
			if (obj == this) {
				return true;
			}

			Quadruple other = obj as Quadruple;
			if (other == null)
				return false;

			if (components[1] == other.components[1] &&
			    components[0] == other.components[0]) {
				return true;
			}
			return false;
		}

		public int CompareTo(object obj) {
			Quadruple other = obj as Quadruple;
			if (obj == null)
				throw new ArgumentException("Cannot compare to null or to a class not instance of '" + typeof (Quadruple) + "'.");

			return CompareTo(other);
		}

		public int CompareTo(Quadruple other) {
			long thish = components[0];
			long thath = other.components[0];

			if (thish < thath)
				return -1;
			if (thish > thath)
				return 1;

			// High 64-bits are equal, so compare low,
			long thisl = components[1];
			long thatl = other.components[1];

			// This comparison needs to be unsigned,
			// True if the signs are different
			bool signdif = (thisl < 0) != (thatl < 0);

			if ((thisl < thatl) ^ signdif)
				return -1;
			if ((thisl > thatl) ^ signdif)
				return 1;

			// Equal,
			return 0;
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("0x{0:x}", High);
			sb.Append(".");
			sb.AppendFormat("0x{0:x}", Low);
			return sb.ToString();
		}
	}
}