using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = true)]
	public sealed class DbTrustedAttribute : Attribute {
	}
}