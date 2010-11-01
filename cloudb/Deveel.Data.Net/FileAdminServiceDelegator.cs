using System;
using System.IO;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public sealed class FileAdminServiceDelegator : IAdminServiceDelegator {
		private readonly string basePath;
				
		private IService manager;
		private IService root;
		private IService block;
		private readonly Logger logger;

		private const string BlockRunFile = "runblock";
		private const string ManagerRunFile = "runmanager";
		private const string RootRunFile = "runroot";
		
		public FileAdminServiceDelegator(string basePath) {
			this.basePath = basePath;
			
			logger = LogManager.NetworkLogger;
		}
		
		public void Init(AdminService adminService) {
			// Start services as necessary,
			try {
				string check_file = Path.Combine(basePath, BlockRunFile);
				if (File.Exists(check_file)) {
					StartService(adminService.Address, ServiceType.Block, adminService.Connector);
				}
				
				check_file = Path.Combine(basePath, ManagerRunFile);
				if (File.Exists(check_file)) {
					StartService(adminService.Address, ServiceType.Manager, adminService.Connector);
				}
				
				check_file = Path.Combine(basePath, RootRunFile);
				if (File.Exists(check_file)) {
					StartService(adminService.Address, ServiceType.Root, adminService.Connector);
				}
			} catch (IOException e) {
				logger.Error("IO Error on Init", e);
				throw;
			}
		}
		
		public IService GetService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager)
				return manager;
			if (serviceType == ServiceType.Root)
				return root;
			if (serviceType == ServiceType.Block)
				return block;
			
			throw new ArgumentException();
		}
		
		public void StartService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector) {
			if (serviceType == ServiceType.Block) {
				string npath = Path.Combine(basePath, "block");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);

				File.Create(Path.Combine(basePath, BlockRunFile));
				block = new FileSystemBlockService(connector, npath);
				block.Start();
			} 
			if (serviceType == ServiceType.Manager) {
				string npath = Path.Combine(basePath, "manager");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, ManagerRunFile));
				manager = new FileSystemManagerService(connector, basePath, npath, address);
				manager.Start();
			} 
			if (serviceType == ServiceType.Root) {
				string npath = Path.Combine(basePath, "root");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, RootRunFile));
				root =  new FileSystemRootService(connector, npath);
				root.Start();
			}
		}
		
		public void StopService(ServiceType serviceType) {
			if (serviceType == ServiceType.Manager && manager != null) {
				manager.Stop();
				manager = null;
				File.Delete(Path.Combine(basePath, ManagerRunFile));
			} else if (serviceType == ServiceType.Root && root != null) {
				root.Stop();
				root = null;
				File.Delete(Path.Combine(basePath, RootRunFile));
			} else if (serviceType == ServiceType.Block && block != null) {
				block.Stop();
				block = null;
				File.Delete(Path.Combine(basePath, BlockRunFile));
			}
		}
		
		public void Dispose() {
			if (manager != null)
				manager.Dispose();
			if (root != null)
				root.Dispose();
			if (block != null)
				block.Dispose();
		}
	}
}