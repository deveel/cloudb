﻿//
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

namespace Deveel.Data.Net {
	public class PathProfile {
		internal PathProfile(IServiceAddress rootAddress, string path, string pathType) {
			this.rootAddress = rootAddress;
			this.path = path;
			this.pathType = pathType;
		}

		private readonly IServiceAddress rootAddress;
		private readonly String path;
		private readonly String pathType;

		public string Path {
			get { return path; }
		}

		public IServiceAddress RootAddress {
			get { return rootAddress; }
		}

		public string PathType {
			get { return pathType; }
		}
	}
}