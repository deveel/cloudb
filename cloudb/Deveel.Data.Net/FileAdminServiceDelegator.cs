using System;
using System.IO;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Net {
	public sealed class FileAdminServiceDelegator : IAdminServiceDelegator {
		private readonly string basePath;
				
		private IService manager;
		private IService root;
		private IService block;
		private Logger logger;

		private const string BlockRunFile = "runblock";
		private const string ManagerRunFile = "runmanager";
		private const string RootRunFile = "runroot";
		
		public FileAdminServiceDelegator(string basePath) {
			this.basePath = basePath;
			
			logger = LogManager.GetLogger("network");
		}
		
		public void Init(AdminService adminService) {
			// Start services as necessary,
			try {
				string check_file = Path.Combine(basePath, BlockRunFile);
				if (File.Exists(check_file)) {
					block = CreateService(adminService.Address, ServiceType.Block, adminService.Connector);
					block.Start();
				}
				
				check_file = Path.Combine(basePath, ManagerRunFile);
				if (File.Exists(check_file)) {
					manager = CreateService(adminService.Address, ServiceType.Manager, adminService.Connector);
					manager.Start();
				}
				
				check_file = Path.Combine(basePath, RootRunFile);
				if (File.Exists(check_file)) {
					root = CreateService(adminService.Address, ServiceType.Root, adminService.Connector);
					root.Start();
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
		
		public IService CreateService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector) {
			if (serviceType == ServiceType.Block) {
				string npath = Path.Combine(basePath, "block");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, BlockRunFile));
				block = new FileSystemBlockService(connector, npath);
				return block;
			} 
			if (serviceType == ServiceType.Manager) {
				string npath = Path.Combine(basePath, "manager");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, ManagerRunFile));
				manager = new FileSystemManagerService(connector, basePath, npath, address);
				return manager;
			} 
			if (serviceType == ServiceType.Root) {
				string npath = Path.Combine(basePath, "root");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, RootRunFile));
				root =  new FileSystemRootService(connector, npath);
				return root;
			}

			throw new ArgumentException();
		}
		
		public void DisposeService(ServiceType serviceType) {
			IService service = GetService(serviceType);
			
			if (service == null)
				throw new InvalidOperationException();

			if (serviceType == ServiceType.Block) {
				File.Delete(Path.Combine(basePath, BlockRunFile));
			} else if (serviceType == ServiceType.Manager) {
				File.Delete(Path.Combine(basePath, ManagerRunFile));
			} else if (serviceType == ServiceType.Root) {
				File.Delete(Path.Combine(basePath, RootRunFile));
			}

			service.Dispose();
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