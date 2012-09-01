using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class MemoryRootService : RootService {
		private readonly Dictionary<string, Stream> pathStreams;
 
		public MemoryRootService(IServiceConnector connector, IServiceAddress address) 
			: base(connector, address) {
			pathStreams = new Dictionary<string, Stream>();
		}

		protected override PathAccess CreatePathAccesss(string pathName) {
			return new MemoryPathAccess(this, pathName);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				foreach (KeyValuePair<string, Stream> pair in pathStreams) {
					pair.Value.Dispose();
				}
			}

			base.Dispose(disposing);
		}

		#region MemoryPathAccess

		class MemoryPathAccess : PathAccess {
			public MemoryPathAccess(RootService service, string pathName) 
				: base(service, pathName) {
			}

			protected override Stream CreatePathStream() {
				MemoryRootService rootService = (MemoryRootService) RootService;
				Stream stream;

				lock (rootService.pathStreams) {
					if (!rootService.pathStreams.TryGetValue(PathName, out stream)) {
						stream = new MemoryStream(1024);
						rootService.pathStreams[PathName] = stream;
					}
				}

				return stream;
			}
		}

		#endregion
	}
}