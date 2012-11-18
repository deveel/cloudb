//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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