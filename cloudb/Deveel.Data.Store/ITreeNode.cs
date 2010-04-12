using System;

namespace Deveel.Data.Store {
	public interface ITreeNode : IDisposable {
		long Id { get; }

		long MemoryAmount { get; }
	}
}