using System;

namespace Deveel.Data {
	public interface IIndexedObjectComparer<T> {
		int Compare(long reference, T value);
	}
}