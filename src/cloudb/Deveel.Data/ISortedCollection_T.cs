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
	/// Defines a <see cref="ICollection{T}">collection</see> that 
	/// is sorted against a given order (specified by the 
	/// provided <see cref="IComparer{T}"/>.
	/// </summary>
	public interface ISortedCollection<T> : ICollection<T> {
		/// <summary>
		/// Gets the instance of the <see cref="IComparer{T}"/> that is
		/// used to sort the elements of the collection.
		/// </summary>
		IComparer<T> Comparer { get; }

		/// <summary>
		/// Gets the first element of the collection in the sorted order.
		/// </summary>
		T First { get; }

		/// <summary>
		/// Gets the least element of the collection in the sorted order.
		/// </summary>
		T Last { get; }


		/// <summary>
		/// Constructs a subset of the collection that starts from
		/// the given element (inclusive) and ends at the bottom of
		/// the list.
		/// </summary>
		/// <param name="start">The starting element of the subset collection.</param>
		/// <returns>
		/// Returns an instance of <see cref="ISortedCollection{T}"/> that
		/// represents a subset of this collection, starting at the
		/// given element (inclusive) and ending at the end of this collection.
		/// </returns>
		ISortedCollection<T> Tail(T start);

		ISortedCollection<T> Head(T end);

		ISortedCollection<T> Sub(T start, T end);
	}
}