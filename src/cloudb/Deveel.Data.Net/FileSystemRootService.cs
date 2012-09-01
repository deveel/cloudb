using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public sealed class FileSystemRootService : RootService {
		private readonly string path;
		private readonly List<string> pathInitializationQueue;


		public FileSystemRootService(IServiceConnector connector, IServiceAddress address, string path) 
			: base(connector, address) {
			this.path = path;

			pathInitializationQueue = new List<string>(64);
		}

		protected override void ProcessInitQueue() {
			lock (PathInitLock) {
				lock (pathInitializationQueue) {
					pathInitializationQueue.Reverse();

					for (int i = pathInitializationQueue.Count - 1; i >= 0; i--) {
						string f = pathInitializationQueue[i];
						try {
							// Load the path info from the managers,
							PathInfo pathInfo = LoadFromManagers(Path.GetFileName(f), -1);
							// Add to the queue,
							AddPathToQueue(pathInfo);
							// Remove the item,
							pathInitializationQueue.RemoveAt(i);
						} catch (Exception e) {
							Logger.Info("Error on path init", e);
							Logger.Info(String.Format("Trying path init {0} later", Path.GetFileName(f)));
						}
					}
				}
			}
		}

		protected override void OnStart() {
			try {

				// Read the manager server address from the properties file,
				Properties p = new Properties();

				// Contains the root properties,
				string propFile = Path.Combine(path, "00.properties");
				if (File.Exists(propFile)) {
					FileStream fin = new FileStream(propFile, FileMode.Open, FileAccess.Read, FileShare.Read);
					p.Load(fin);
					fin.Close();
				}

				// Fetch the manager server property,
				String v = p.GetProperty("manager_server_address");
				if (v != null) {
					String[] addresses = v.Split(',');
					int sz = addresses.Length;
					ManagerServices = new IServiceAddress[sz];
					for (int i = 0; i < sz; ++i) {
						ManagerServices[i] = ServiceAddresses.ParseString(addresses[i]);
					}
				}

			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}

			// Adds all the files to the path info queue,
			string[] rootFiles = Directory.GetFiles(path);
			foreach (string f in rootFiles) {
				String fname = Path.GetFileName(f);
				if (!fname.Contains(".")) {
					pathInitializationQueue.Add(f);
				}
			}

			base.OnStart();
		}

		protected override PathAccess CreatePathAccesss(string pathName) {
			return new FilePathAccess(this, pathName);
		}

		protected override void OnManagersSet(IServiceAddress[] addresses) {
			StringBuilder b = new StringBuilder();
			for (int i = 0; i < addresses.Length; ++i) {
				b.Append(addresses[i].ToString());
				if (i < addresses.Length - 1) {
					b.Append(",");
				}
			}

			// Write the manager server address to the properties file,
			Properties p = new Properties();
			p.SetProperty("manager_server_address", b.ToString());

			// Contains the root properties,
			string propFile = Path.Combine(path, "00.properties");
			FileStream fout = new FileStream(propFile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
			p.Store(fout, null);
			fout.Close();

			base.OnManagersSet(addresses);
		}

		protected override void OnManagersClear() {
			// Write the manager server address to the properties file,
			Properties p = new Properties();
			p.SetProperty("manager_server_address", "");

			// Contains the root properties,
			string propFile = Path.Combine(path, "00.properties");
			FileStream fout = new FileStream(propFile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
			p.Store(fout, null);
			fout.Close();
		}

		#region FilePathAccess

		class FilePathAccess : PathAccess {
			public FilePathAccess(FileSystemRootService service, string pathName) 
				: base(service, pathName) {
			}

			protected override Stream CreatePathStream() {
				string path = ((FileSystemRootService) RootService).path;
				return new FileStream(System.IO.Path.Combine(path, PathName), FileMode.OpenOrCreate, FileAccess.ReadWrite,
				                      FileShare.None, 2048, FileOptions.WriteThrough);
			}
		}

		#endregion
	}
}