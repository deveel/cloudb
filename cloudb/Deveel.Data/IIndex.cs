using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface IIndex : IEnumerable<long> {
		long Count { get; }

		long this[long offset] { get; }



		void Clear();

		void Clear(long offset, long size);

		long SearchFirst<T>(T value, IIndexedObjectComparer<T> c);

		long SearchLast<T>(T value, IIndexedObjectComparer<T> c);


		IIndexCursor GetCursor(long start, long end);

		IIndexCursor GetCursor();

		void Insert<T>(T value, long index, IIndexedObjectComparer<T> c);

		bool InsertUnique<T>(T value, long index, IIndexedObjectComparer<T> c);

		void Remove<T>(T value, long index, IIndexedObjectComparer<T> c);

		void Add(long index);

		void Insert(long index, long offset);

		long RemoveAt(long offset);

		void InsertSortKey(long index);

		void RemoveSortKey(long index);

		bool ContainsSortKey(long index);
	}
}