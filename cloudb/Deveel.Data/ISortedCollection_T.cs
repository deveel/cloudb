using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface ISortedCollection<T> : ICollection<T> {
		IComparer<T> Comparer { get; }

		T First { get; }

		T Last { get; }


		ISortedCollection<T> Tail(T start);

		ISortedCollection<T> Head(T end);

		ISortedCollection<T> Sub(T start, T end);
	}
}