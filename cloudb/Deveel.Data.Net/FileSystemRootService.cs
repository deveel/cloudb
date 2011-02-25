using System;
using System.IO;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public sealed class FileSystemRootService : RootService {
		public FileSystemRootService(IServiceConnector connector, IServiceAddress address, string basePath)
			: base(connector, address) {
			this.basePath = basePath;
		}

		private readonly string basePath;

		protected override void OnStart() {
			try {
				// Read the manager service address from the properties file,
				Properties p = new Properties();

				// Contains the root properties,
				string propFile = Path.Combine(basePath, "00.properties");
				if (File.Exists(propFile)) {
					using (FileStream fin = new FileStream(propFile, FileMode.Open, FileAccess.Read)) {
						p.Load(fin);
					}
				}

				// Fetch the manager service property,
				string v = p.GetProperty("manager_address");
				if (v != null) {
					string[] sp = v.Split(',');
					IServiceAddress[] addresses = new IServiceAddress[sp.Length];
					for (int i = 0; i < sp.Length; i++) {
						addresses[i] = ServiceAddresses.ParseString(sp[i].Trim());
					}
					ManagerAddresses = addresses;
				}
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		protected override PathAccess CreatePathAccess(string pathName) {
			return new FileBasedPathAccess(this, pathName);
		}

		protected override void OnBindingWithManager(IServiceAddress[] managerAddress) {
			// Contains the root properties,
			string propFile = Path.Combine(basePath, "00.properties");
			using(FileStream fileStream = new FileStream(propFile, FileMode.OpenOrCreate, FileAccess.ReadWrite)) {
				// Write the manager server address to the properties file,
				Properties p = new Properties();
				if (fileStream.Length > 0)
					p.Load(fileStream);

				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < managerAddress.Length; i++) {
					sb.Append(managerAddress[i].ToString());
					if (i < managerAddress.Length - 1)
						sb.Append(',');
				}

				p.SetProperty("manager_address", sb.ToString());

				fileStream.SetLength(0);
				p.Store(fileStream, null);
				fileStream.Close();
			}
		}

		protected override void OnUnbindingWithManager(IServiceAddress[] managerAddress) {
			// Contains the root properties,
			string propFile = Path.Combine(basePath, "00.properties");
			using (FileStream fileStream = new FileStream(propFile, FileMode.OpenOrCreate, FileAccess.ReadWrite)) {
				// Write the manager server address to the properties file,
				Properties p = new Properties();
				if (fileStream.Length > 0)
					p.Load(fileStream);

				p.Remove("manager_address");

				fileStream.SetLength(0);
				p.Store(fileStream, null);
				fileStream.Close();
			}
		}

		#region FileBasedPathAccess

		private class FileBasedPathAccess : PathAccess {
			private readonly FileSystemRootService service;

			public FileBasedPathAccess(FileSystemRootService service, string name)
				: base(name) {
				this.service = service;
			}

			protected override bool HasLocalData {
				get {
					string pathDataFile = System.IO.Path.Combine(service.basePath, Name);
					return File.Exists(pathDataFile);
				}
			}

			protected internal override void Open() {
				lock (AccessLock) {
					if (AccessStream == null) {
						Stream accessStream = new FileStream(System.IO.Path.Combine(service.basePath, Name), FileMode.OpenOrCreate,
						                                     FileAccess.ReadWrite, FileShare.None);
						SetAccessStream(accessStream);
					}
				}
			}
		}

		#endregion
	}
}