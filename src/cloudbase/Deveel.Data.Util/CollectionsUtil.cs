using System;
using System.Collections.Generic;

namespace Deveel.Data.Util {
	static class CollectionsUtil {
		public static Int32 BinarySearch<T>(IList<T> list, T value) {
			return BinarySearch(list, value, Comparer<T>.Default);
		}

		public static Int32 BinarySearch<T>(IList<T> list, T value, IComparer<T> comparer) {
			#region Parameter Validation

			if (ReferenceEquals(null, list))
				throw new ArgumentNullException("list");
			if (ReferenceEquals(null, comparer))
				throw new ArgumentNullException("comparer");

			#endregion

			Int32 lower = 0;
			Int32 upper = list.Count - 1;

			while (lower <= upper) {
				Int32 middle = (lower + upper) / 2;
				Int32 comparisonResult = comparer.Compare(value, list[middle]);
				if (comparisonResult == 0)
					return middle;
				if (comparisonResult < 0)
					upper = middle - 1;
				else
					lower = middle + 1;
			}

			return -1;
		}
	}
}