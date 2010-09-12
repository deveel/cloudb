using System;
using System.IO;
using System.Net;

namespace Deveel.Data.Net {
	public sealed class TcpFileAdminService : TcpAdminService {
		private readonly string basePath;

		private const string BlockRunFile = "runblock";
		private const string ManagerRunFile = "runmanager";
		private const string RootRunFile = "runroot";

		public TcpFileAdminService(NetworkConfigSource config, IPAddress address, int port, string password, string basePath) 
			: base(config, address, port, password) {

			if (!Directory.Exists(basePath))
				Directory.CreateDirectory(basePath);

			this.basePath = basePath;
		}

		protected override void OnInit() {
			// Start services as necessary,
			try {
				string check_file = Path.Combine(basePath, BlockRunFile);
				if (File.Exists(check_file))
					InitService(ServiceType.Block);

				check_file = Path.Combine(basePath, ManagerRunFile);
				if (File.Exists(check_file))
					InitService(ServiceType.Manager);

				check_file = Path.Combine(basePath, RootRunFile);
				if (File.Exists(check_file))
					InitService(ServiceType.Root);
			} catch (IOException) {
				//TODO: ERROR log ...
				throw;
			}

			base.OnInit();
		}

		protected override IService CreateService(ServiceType serviceType) {
			if (serviceType == ServiceType.Block) {
				string npath = Path.Combine(basePath, "block");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, BlockRunFile));
				return new FileSystemBlockService(Connector, npath);
			} 
			if (serviceType == ServiceType.Manager) {
				string npath = Path.Combine(basePath, "manager");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, ManagerRunFile));
				return new FileSystemManagerService(Connector, basePath, npath, Address);
			} 
			if (serviceType == ServiceType.Root) {
				string npath = Path.Combine(basePath, "root");
				if (!Directory.Exists(npath))
					Directory.CreateDirectory(npath);
				File.Create(Path.Combine(basePath, RootRunFile));
				return new FileSystemRootService(Connector, npath);
			}

			throw new ArgumentException();
		}

		protected override void DisposeService(IService service) {
			ServiceType serviceType = service.ServiceType;

			if (serviceType == ServiceType.Block) {
				File.Delete(Path.Combine(basePath, BlockRunFile));
			} else if (serviceType == ServiceType.Manager) {
				File.Delete(Path.Combine(basePath, ManagerRunFile));
			} else if (serviceType == ServiceType.Root) {
				File.Delete(Path.Combine(basePath, RootRunFile));
			}

			service.Dispose();
		}
	}
}