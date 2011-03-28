using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface IPathResponse {
		IPathRequest Request { get; }

		int StatusCode { get; }

		IList<Attribute> Attributes { get; }

		Stream InputStream { get; }
	}
}