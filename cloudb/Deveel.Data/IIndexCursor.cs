using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface IIndexCursor : IEnumerator<long>, ICloneable {
		long Count { get; }

		long Position { get; set; }


		bool MoveBack();

		long Remove();
	}
}