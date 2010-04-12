using System;

namespace Deveel.Data {
	public interface INumberEnumerator : IInteractiveEnumerator<long>, ICloneable {
		long Count { get; }

		long Position { get; set; }


		bool MovePrevious();

		new long Remove();
	}
}