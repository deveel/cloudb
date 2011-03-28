using System;

namespace Deveel.Data.Net.Client {
	public enum PathValueType : byte {
		Null = 0,
		Boolean = 1,
		Byte = 2,
		Int16 = 3,
		Int32 = 4,
		Int64 = 5,
		Single = 6,
		Double = 7,
		DateTime = 8,
		String = 9,
		Struct = 20,
		Array = 30
	}
}