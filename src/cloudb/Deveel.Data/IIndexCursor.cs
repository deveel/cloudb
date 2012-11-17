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

namespace Deveel.Data {
	/// <summary>
	/// Enumerates the elements of an <see cref="IIndex"/> instance 
	/// and provides additional functionalities to move backward, 
	/// modify the content of the index, freely set the position
	/// within the enumeration and obtain more information about 
	/// the set enumerated.
	/// </summary>
	public interface IIndexCursor : IEnumerator<long>, ICloneable {
		/// <summary>
		/// Gets the total number of values in the group defined by 
		/// the scope of the enumerator.
		/// </summary>
		long Count { get; }

		/// <summary>
		/// Gets or sets the current zero-based offset in the
		/// enumerator address space.
		/// </summary>
		/// <value>
		/// The returned value is the current position of the
		/// cursor within the enumeration, -1 if the cursor is
		/// located before the start of the enumeration, or
		/// <see cref="Count"/> if the cursor is at the end of
		/// the enumeration.
		/// </value>
		long Position { get; set; }

		/// <summary>
		/// Decreses the cursor of the enumeration to the previous
		/// position within the enumeration.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the cursor successfully stepped back
		/// by one within the enumeration, or <b>false</b> otherwise (for
		/// example, when at the beginning of the enumeration).
		/// </returns>
		bool MoveBack();

		/// <summary>
		/// Removes a single element from the underlying index
		/// at the current position of the enumeration.
		/// </summary>
		/// <remarks>
		/// When this method returns the interator position will point to 
		/// the next value in the collection.
		/// </remarks>
		/// <returns>
		/// Returns the element removed from the enumeration.
		/// </returns>
		/// <seealso cref="Position"/>
		/// <seealso cref="MoveNext" />
		/// <seealso cref="MoveBack" />
		long Remove();
	}
}