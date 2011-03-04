using System;

namespace Deveel.Data.Net.Client {
	public enum RequestType : byte {
		Get = 1,
		Put = 2,
		Post = 3,
		Delete = 4
	}
}