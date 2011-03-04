using System;

namespace Deveel.Data.Net.Client {
	public interface IRestMessageSerializer : ITextMessageSerializer {
		RestFormat Format { get; }
	}
}