using System;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net.Serialization {
	public interface IRestMessageSerializer : ITextMessageSerializer {
		RestFormat Format { get; }
	}
}