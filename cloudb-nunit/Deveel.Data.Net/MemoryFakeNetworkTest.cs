using System;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public sealed class MemoryFakeNetworkTest : FakeNetworkTest {
		protected override FakeNetworkStoreType StoreType {
			get { return FakeNetworkStoreType.Memory; }
		}
	}
}