using System;
using System.IO;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public sealed class FileSystemFakeNetworkTest : FakeNetworkTest {
		private readonly object fileLock = new Object();
		private string path;
		
		protected override FakeNetworkStoreType StoreType {
			get { return FakeNetworkStoreType.FileSystem; }
		}
		
		protected override void Config(ConfigSource config) {
			lock(fileLock) {
				path = Path.Combine(Environment.CurrentDirectory, "base");
				if (Directory.Exists(path))
					Directory.Delete(path, true);
			
				Directory.CreateDirectory(path);
			
				config.SetValue("node_directory", path);
				base.Config(config);
			}
		}
		
		protected override void OnTearDown() {
			if (Directory.Exists(path))
				Directory.Delete(path, true);
		}
	}
}