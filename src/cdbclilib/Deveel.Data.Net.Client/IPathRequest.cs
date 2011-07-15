using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net.Client {
	public interface IPathRequest {
		IPathTransaction Transaction { get; }

		RequestType Type { get; }

		IList<Attribute> Attributes { get; }

		Stream OutputStream { get; }


		IPathResponse Send();
	}
}