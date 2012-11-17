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

namespace Deveel.Data {
	/// <summary>
	/// An object used to search within an index, comparing the value
	/// specified with the value fetched from the underlying index,
	/// using the reference passed.
	/// </summary>
	public interface IIndexedObjectComparer<T> {
		/// <summary>
		/// Compares the given value with the value fetched
		/// from the underlying index, identified by the given
		/// reference.
		/// </summary>
		/// <param name="reference">The reference to the value to
		/// use as first term of the comparison.</param>
		/// <param name="value">The value used as second term of
		/// the comparison.</param>
		/// <returns>
		/// Returns 0 if the two values are equal; 1 if the second value
		/// is greater than the first value; -1 if the first value is greater
		/// than the second value.
		/// </returns>
		int Compare(long reference, T value);
	}
}