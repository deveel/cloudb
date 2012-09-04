using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class TrustedAttribute : Attribute {
		
	}
}