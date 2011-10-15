using System;

namespace Deveel.Data.Net.Security {
	/// <summary>
	/// Enumerates the type of a single data element in a
	/// authorization request/response. 
	/// </summary>
	public enum AuthDataType {
		Null,
		Boolean,
		Number,
		String,
		DateTime,
		Binary,
		List
	}
}