using System;

namespace Deveel.Data.Net.Client {
	internal interface IAttributesHandler {
		bool IsReadOnly { get; }

		ActionAttributes Attributes { get; }
	}
}