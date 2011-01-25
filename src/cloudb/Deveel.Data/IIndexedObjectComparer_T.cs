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