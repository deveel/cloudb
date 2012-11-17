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

namespace Deveel.Data.Net {
	public class MachineProfile {
		private readonly IServiceAddress address;
		private MachineRoles roles;
		
		private long memoryUsed;
		private long memoryTotal;
		private long diskUsed;
		private long diskTotal;

		private String errorMessage;

		internal MachineProfile(IServiceAddress address) {
			this.address = address;
		}

		public IServiceAddress ServiceAddress {
			get { return address; }
		}

		public MachineRoles Roles {
			get { return roles; }
			internal set { roles = value; }
		}

		public bool IsBlock {
			get { return (roles & MachineRoles.Block) != 0; }
		}

		public bool IsRoot {
			get { return (roles & MachineRoles.Root) != 0; }
		}

		public bool IsManager {
			get { return (roles & MachineRoles.Manager) != 0; }
		}

		internal bool IsNotAssigned {
			get { return roles == MachineRoles.None; }
		}

		public bool IsError {
			get { return (errorMessage != null); }
		}

		public string ErrorMessage {
			get { return errorMessage; }
			internal set { errorMessage = value; }
		}

		public long MemoryUsed {
			get { return memoryUsed; }
			internal set { memoryUsed = value; }
		}

		public long MemoryTotal {
			get { return memoryTotal; }
			internal set { memoryTotal = value; }
		}

		public long DiskUsed {
			get { return diskUsed; }
			internal set { diskUsed = value; }
		}

		public long DiskTotal {
			get { return diskTotal; }
			internal set { diskTotal = value; }
		}

		public bool IsInRole(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager)
				return IsManager;
			if (serviceType == ServiceType.Root)
				return IsRoot;
			if (serviceType == ServiceType.Block)
				return IsBlock;

			return false;
		}
	}
}