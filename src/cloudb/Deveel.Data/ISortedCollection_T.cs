using System;
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// Defines a <see cref="ICollection">collection</see> that 
	/// is sorted against a given order (specified by the 
	/// provided <see cref="IComparer"/>.
	/// </summary>
	public interface ISortedCollection<T> : ICollection<T> {
		/// <summary>
		/// Gets the instance of the <see cref="IComparer"/> that is
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
		/// Returns an instance of <see cref="ISortedCollcetion"/> that
		/// represents a subset of this collection, starting at the
		/// given element (inclusive) and ending at the end of this collection.
		/// </returns>
		ISortedCollection<T> Tail(T start);

		ISortedCollection<T> Head(T end);

		ISortedCollection<T> Sub(T start, T end);
	}
}