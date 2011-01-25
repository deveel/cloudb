using System;
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// An index of long integer numbers (64 bits), providing an architecture
	/// to fastly insert and search elements.
	/// </summary>
	/// <remarks>
	/// The typical use of an implementation of <see cref="IIndex"/> is to provide
	/// methods to sort and search more complex data elements, using the integer
	/// numbers as pointers to external data sources.
	/// </remarks>
	public interface IIndex : IEnumerable<long> {
		/// <summary>
		/// Gets the total number of elements in the index.
		/// </summary>
		long Count { get; }

		/// <summary>
		/// Gets the long integer element at the given position in the index.
		/// </summary>
		/// <param name="offset">The zero-based offset of the element to get.</param>
		/// <remarks>
		/// Note that in many implementations this will not be an O(1) operation 
		/// so attention should be paid to the implementation details. In cases 
		/// where it is intended to read a sequence of values from the list, use 
		/// a cursor.
		/// </remarks>
		/// <returns></returns>
		long this[long offset] { get; }


		/// <summary>
		/// Clears all values of the index.
		/// </summary>
		void Clear();

		/// <summary>
		/// Clears the values of the index from the given starting offset
		/// for the given number of elements.
		/// </summary>
		/// <param name="offset">The offset from where to start clearing the index.</param>
		/// <param name="size">The number of elements to clear.</param>
		void Clear(long offset, long size);

		/// <summary>
		/// Searches for the first instance of the specified value within the index.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="value">The higher order object to search for.</param>
		/// <param name="c">The comparer between higher order objects and the 
		/// long integer elements in the index.</param>
		/// <remarks>
		/// The correct operation of this function depends on the index maintaining a 
		/// consistent order through its lifetime.  If the collation characteristics 
		/// change then the result of this function is undefined.
		/// <para>
		/// <b>Note:</b> This function assumes the index is sorted.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the position of the first value in the index or returns 
		/// -(position + 1) if the specified value is not found and position 
		/// is the place where the value would be inserted to maintain index 
		/// integrity (sort order).
		/// </returns>
		long SearchFirst<T>(T value, IIndexedObjectComparer<T> c);

		/// <summary>
		/// Searches for the last instance of the specified value within the index.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="value">The higher order object to search for.</param>
		/// <param name="c">The comparator between higher order objects and the 
		/// long integer elements in the index.</param>
		/// <remarks>
		/// The correct operation of this function depends on the index maintaining a 
		/// consistent order through its lifetime.  If the collation characteristics 
		/// change then the result of this function is undefined.
		/// <para>
		/// <b>Note:</b> This function assumes the index is sorted.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the position of the last value in the index or returns 
		/// -(position + 1) if the specified value is not found and position 
		/// is the place where the value would be inserted to maintain index 
		/// integrity (sort order).
		/// </returns>
		long SearchLast<T>(T value, IIndexedObjectComparer<T> c);

		/// <summary>
		/// Gets an instance of <see cref="IIndexCursor"/> that is limited between 
		/// the boundaries specified and allows for the access of all elements in 
		/// the subset if the index.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: while a cursor exists it is assumed that no modifications will be 
		/// made to the index. If the index changes while a cursor is active the behavior 
		/// is unspecified.
		/// </remarks>
		/// <param name="start">The start the offset of the first position (inclusive).</param>
		/// <param name="end">The offset of the last position (inclusive).</param>
		/// <returns></returns>
		IIndexCursor GetCursor(long start, long end);

		/// <summary>
		/// Gets an instance of <see cref="IIndexCursor"/> that iterates through all the
		/// elements of the index.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: while a cursor exists it is assumed that no modifications will be 
		/// made to the index. If the index changes while a cursor is active the behavior 
		/// is unspecified.
		/// </remarks>
		/// <returns></returns>
		IIndexCursor GetCursor();

		/// <summary>
		/// Inserts a long integer element into the index at the ordered position.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="value">The value the higher order object to insert.</param>
		/// <param name="index">The long integer value to insert into the index and 
		/// which maps to <paramref name="value"/>.</param>
		/// <param name="c">The comparer between higher order objects and the long integer
		/// elements in the index.</param>
		/// <remarks>
		/// If there are elements in the index that are the same value as the one being 
		/// inserted, the element is inserted after the end of the group of equal elements 
		/// in the set.
		/// <para>
		/// The correct operation of this function depends on the index maintaining 
		/// a consistent order through its lifetime. If the collation characteristics 
		/// change then the result of this function is undefined.
		/// </para>
		/// </remarks>
		void Insert<T>(T value, long index, IIndexedObjectComparer<T> c);

		bool InsertUnique<T>(T value, long index, IIndexedObjectComparer<T> c);

		void Remove<T>(T value, long index, IIndexedObjectComparer<T> c);

		/// <summary>
		/// Adds a long integer value to the end of the index ignoring any 
		/// ordering scheme that may have previously been used to insert 
		/// values into the index. 
		/// </summary>
		/// <param name="value">The numeric value to add.</param>
		void Add(long value);

		/// <summary>
		/// Inserts a long integer value at the given position in the list 
		/// shifting any values after the position forward by one.
		/// </summary>
		/// <param name="value">The value to insert into the index.</param>
		/// <param name="offset">The offset within the index where to insert 
		/// the given value.</param>
		void Insert(long value, long offset);

		/// <summary>
		/// Removes a value from the given offset within the index, shifting any 
		/// values after the given offset backwards by one.
		/// </summary>
		/// <param name="offset">The offset within the offset from where removing.</param>
		/// <returns>
		/// Returns the value removed from the index.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is smaller than 0 or greather
		/// than the count of elements in the index.
		/// </exception>
		/// <exception cref="ApplicationException">
		/// If the index is read-only and the value cannot be removed.
		/// </exception>
		long RemoveAt(long offset);

		/// <summary>
		/// Inserts a long integer value at an ordered position in the index 
		/// where the order is the ascending collation of integer values.
		/// </summary>
		/// <param name="value">The numeric value to insert into the index.</param>
		/// <remarks>
		/// The correct operation of this function depends on the index maintaining 
		/// a consistent order through its lifetime. If the collation characteristics 
		/// change then the result of this function is undefined.
		/// </remarks>
		void InsertSortKey(long value);

		/// <summary>
		/// Removes the first long integer value from the order position in the 
		/// index where the order is the ascending collation of the integer values.
		/// </summary>
		/// <param name="value">The numeric value to remove from the index.</param>
		/// <remarks>
		/// The correct operation of this function depends on the index maintaining 
		/// a consistent order through its lifetime. If the collation characteristics 
		/// change then the result of this function is undefined.
		/// </remarks>
		void RemoveSortKey(long value);

		/// <summary>
		/// Checks if the set contains the given integer value assuming the order of 
		/// the set is the ascending collation of the integer values.
		/// </summary>
		/// <param name="value">The numeric value to check.</param>
		/// <remarks>
		/// The correct operation of this function depends on the index maintaining 
		/// a consistent order through its lifetime. If the collation characteristics 
		/// change then the result of this function is undefined.
		/// </remarks>
		/// <returns>
		/// Returns true if the set contains the given integer value, or false if the 
		/// value was not found.
		/// </returns>
		bool ContainsSortKey(long value);
	}
}