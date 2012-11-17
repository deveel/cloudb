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
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FileSystemServiceFactory : IServiceFactory {
		private readonly string basePath;
		private ManagerService manager;
		private RootService root;
		private BlockService block;

		private const string BlockRunFile = "runblock";
		private const string ManagerRunFile = "runmanager";
		private const string RootRunFile = "runroot";

		public FileSystemServiceFactory(string basePath) {
			this.basePath = basePath;
		}

		private Service DoCreateService(IServiceAddress serviceAddress, ServiceType serviceType, IServiceConnector connector) {
			Service service = null;
			if (serviceType == ServiceType.Manager) {
				if (manager == null) {
					string npath = Path.Combine(basePath, "manager");
					if (!Directory.Exists(npath))
						Directory.CreateDirectory(npath);

					manager = new FileSystemManagerService(connector, basePath, npath, serviceAddress);
				}

				service = manager;
			} else if (serviceType == ServiceType.Root) {
				if (root == null) {
					string npath = Path.Combine(basePath, "root");
					if (!Directory.Exists(npath))
						Directory.CreateDirectory(npath);

					root = new FileSystemRootService(connector, serviceAddress, npath);
				}

				service = root;
			} else if (serviceType == ServiceType.Block) {
				if (block == null) {
					string npath = Path.Combine(basePath, "block");
					if (!Directory.Exists(npath))
						Directory.CreateDirectory(npath);

					block = new FileSystemBlockService(connector, npath);
				}

				service = block;
			}

			if (service != null) {
				service.Started += ServiceStarted;
				service.Stopped += ServiceStopped;
			}

			return service;
		}

		public void Init(AdminService adminService) {
			string checkFile = Path.Combine(basePath, BlockRunFile);
			if (File.Exists(checkFile))
				adminService.StartService(ServiceType.Block);

			checkFile = Path.Combine(basePath, ManagerRunFile);
			if (File.Exists(checkFile))
				adminService.StartService(ServiceType.Manager);

			checkFile = Path.Combine(basePath, RootRunFile);
			if (File.Exists(checkFile))
				adminService.StartService(ServiceType.Root);
		}

		public IService CreateService(IServiceAddress serviceAddress, ServiceType serviceType, IServiceConnector connector) {
			return DoCreateService(serviceAddress, serviceType, connector);
		}

		private void ServiceStopped(object sender, EventArgs e) {
			if (sender is ManagerService) {
				File.Delete(Path.Combine(basePath, ManagerRunFile));
			} else if (sender is RootService) {
				File.Delete(Path.Combine(basePath, RootRunFile));
			} else if (sender is BlockService) {
				File.Delete(Path.Combine(basePath, BlockRunFile));
			}
		}

		private void ServiceStarted(object sender, EventArgs e) {
			if (sender is ManagerService)
				using (File.Open(Path.Combine(basePath, ManagerRunFile), FileMode.OpenOrCreate)) return;
            if (sender is RootService)
                using (File.Open(Path.Combine(basePath, RootRunFile), FileMode.OpenOrCreate)) return;
			if (sender is BlockService)
				using (File.Open(Path.Combine(basePath, BlockRunFile), FileMode.OpenOrCreate)) return;
		}
	}
}