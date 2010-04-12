using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface ISortedList<T> : IList<T> {
		long SearchFirst(T value, IIndexedObjectComparer<T> comparer);

		long SearchLast(T value, IIndexedObjectComparer<T> comparer);

		bool Remove(T value, long reference, IIndexedObjectComparer<T> comparer);

		bool RemoveSort(T value);

		void InsertSort(T value);

		void Insert(T value, long reference, IIndexedObjectComparer<T> comparer);

		bool InsertUnique(T value, long reference, IIndexedObjectComparer<T> comparer);

		bool ContainsSort(T value);
	}
}