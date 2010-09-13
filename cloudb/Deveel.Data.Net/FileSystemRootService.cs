using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Net {
	public sealed class FileSystemRootService : RootService {
		public FileSystemRootService(IServiceConnector connector, string basePath)
			: base(connector) {
			this.basePath = basePath;
		}
		
		private string basePath;
		
		protected override void OnInit() {
			try {
				// Read the manager service address from the properties file,
				Util.Properties p = new Util.Properties();

				// Contains the root properties,
				string propFile = Path.Combine(basePath, "00.properties");
				if (File.Exists(propFile)) {
					using (FileStream fin = new FileStream(propFile, FileMode.Open, FileAccess.Read)) {
						p.Load(fin);
					}
				}

				// Fetch the manager service property,
				string v = p.GetProperty("manager_server_address");
				if (v != null) {
					ManagerAddress = ServiceAddresses.ParseString(v);
				}
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}

		}
		
		protected override void CreatePath(string pathName, string pathTypeName) {
			string f = Path.Combine(basePath, pathName);
			FileInfo fileInfo = new FileInfo(f);
			if (fileInfo.Exists)
				throw new ApplicationException("Path file for '" + pathName + "' exists on this root service.");

			// Create the root file
			using (fileInfo.Create()) {
				// immediately call Dispose on the stream...
			}

			// Create a summary file for storing information about the path
			string summaryFile = Path.Combine(basePath, pathName + ".summary");
			Util.Properties p = new Util.Properties();
			p.SetProperty("path_type", pathTypeName);
			using (FileStream fileStream = new FileStream(summaryFile, FileMode.CreateNew, FileAccess.Write)) {
				p.Store(fileStream, null);
			}
		}
		
		protected override void DeletePath(string pathName) {
			// Check the file exists,
			string f = Path.Combine(basePath, pathName);
			if (!File.Exists(f))
				throw new ApplicationException("Path file for '" + pathName + "' doesn't exist on this root service.");
			
			// We simply add a '.delete' file to indicate it's deleted
			string delFile = Path.Combine(basePath, pathName + ".deleted");
			File.Create(delFile);
		}
		
		protected override PathAccess FetchPathAccess(string pathName) {
			// Read it from the file system.
			string f = Path.Combine(basePath, pathName);
			
			// If it doesn't exist, generate an error
			if (!File.Exists(f))
				throw new ApplicationException("Path '" + pathName + "' not found input this root service.");
			
			// If it does exist, does the .deleted file exist indicating this root
			// path was removed,
			if (File.Exists(Path.Combine(basePath, pathName + ".deleted")))
				throw new ApplicationException("Path '" + pathName + "' did exist but was deleted.");
			
			// Read the summary data for this path.
			string summaryFile = Path.Combine(basePath, pathName + ".summary");
			
			Util.Properties p = new Util.Properties();
			
			using (FileStream fileStream = new FileStream(summaryFile, FileMode.Open, FileAccess.Read)) {
				p.Load(fileStream);
			}

			string pathType = p.GetProperty("path_type");

			// Format it into a PathAccess object,
			FileStream accessStream = new FileStream(f, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 1024, FileOptions.WriteThrough);
			return new PathAccess(accessStream, pathName, pathType);
		}

		protected override IList<PathStatus> ListPaths() {
			List<PathStatus> list = new List<PathStatus>();
			string[] all_files = Directory.GetFiles(basePath);
			foreach (string file in all_files) {
				string fname = Path.GetFileNameWithoutExtension(file);
				string ext = Path.GetExtension(file);
				bool deleted = false;
				if (ext.Equals(".deleted")) {
					deleted = true;
				} else if (fname.EndsWith(".summary") ||
						   fname.EndsWith(".properties")) {
					continue;
				}

				list.Add(new PathStatus(fname, deleted));
			}

			return list;
		}
	}
}