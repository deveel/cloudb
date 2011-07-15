using System;

namespace Deveel.Data.Net.Serialization {
	public interface IRpcMessageSerializer : IMessageSerializer {
		bool SupportsMessageStream { get; }
	}
}