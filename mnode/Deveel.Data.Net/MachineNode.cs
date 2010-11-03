using System;
using System.Collections.Specialized;
using System.Configuration.Install;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

#if UNIX
using Mono.Unix.Native;
#endif
using System.ServiceProcess;
using System.Threading;

using Deveel.Configuration;
using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data.Net {
	public static class MachineNode {
		private static TcpAdminService service = null;

#if WIN32
		private static HandlerRoutine ExitCallback;
#endif

		private static AutoResetEvent waitHandle;
				
		private static Options GetOptions() {
			Options options = new Options();
			options.AddOption("nodeconfig", true, "The node configuration file (default: node.conf).");
			options.AddOption("netconfig", true, "The network configuration file (default: network.conf).");
			options.AddOption("host", true, "The interface address to bind the socket on the local machine " +
							  "(optional - if not given binds to all interfaces)");
			options.AddOption("port", true, "The port to bind the socket.");
			options.AddOption("install", false, "Installs the node as a service in this machine");
			options.AddOption("user", true, "The user name for the authorization credentials to install/uninstall " +
			                 "the service.");
			options.AddOption("password", true, "The password credential used to authorize installation and " +
			                 "uninstallation of the service in this machine.");
			options.AddOption("uninstall", false, "Uninstalls a service for the node that was previously installed.");
			options.AddOption("storage", true, "The type of storage used to persist node information and data");
			options.AddOption("protocol", true, "The connection protocol used by this node to listen connections");
			return options;
		}
		
		private static IServiceFactory GetServiceFactory(string storage, ConfigSource nodeConfigSource) {
			if (storage == "file") {
				string nodeDir = nodeConfigSource.GetString("node_directory", Environment.CurrentDirectory);
				return new FileSystemServiceFactory(nodeDir);
			}
			if (storage == "memory")
				return new MemoryServiceFactory();
			
			if (String.IsNullOrEmpty(storage) &&
			   	nodeConfigSource != null) {
				storage = nodeConfigSource.GetString("storage", "file");
				return GetServiceFactory(storage, nodeConfigSource);
			}
			
			return null;
		}
		
		private static void SetEventHandlers() {
#if WIN32
			ExitCallback = new HandlerRoutine(ConsoleCtrlCheck);
			SetConsoleCtrlHandler(ExitCallback, true);
#elif UNIX			
			System.Threading.Thread signalThread = new System.Threading.Thread(CheckSignal);
			signalThread.Start();
#endif
		}

		[STAThread]
		private static int Main(string[] args) {
			SetEventHandlers();
			
			ProductInfo libInfo = ProductInfo.GetProductInfo(typeof(TcpAdminService));
			ProductInfo nodeInfo = ProductInfo.GetProductInfo(typeof(MachineNode));

			Console.Out.WriteLine("{0} {1} ( {2} )", nodeInfo.Title, nodeInfo.Version, nodeInfo.Copyright);
			Console.Out.WriteLine(nodeInfo.Description);
			Console.Out.WriteLine();
			Console.Out.WriteLine("{0} {1} ( {2} )", libInfo.Title, libInfo.Version, libInfo.Copyright);

			string nodeConfig = null, netConfig = null;
			string hostArg = null, portArg = null;

			StringWriter wout = new StringWriter();
			Options options = GetOptions();

			CommandLine commandLine = null;

			bool failed = false;

			try {
				ICommandLineParser parser = new GnuParser(options);
				commandLine = parser.Parse(args);

				nodeConfig = commandLine.GetOptionValue("nodeconfig", "node.conf");
				netConfig = commandLine.GetOptionValue("netconfig", "network.conf");
				hostArg = commandLine.GetOptionValue("host");
				portArg = commandLine.GetOptionValue("port");
			} catch(ParseException) {
				wout.WriteLine("Error parsing arguments.");
				failed = true;
			}

			if (commandLine.HasOption("install")) {
				try {
					Install(commandLine.GetOptionValue("user"), commandLine.GetOptionValue("password"));
				} catch(Exception e) {
					Console.Error.WriteLine("Error installing service: " + e.Message);
					return 1;
				}
			} else if (commandLine.HasOption("uninstall")) {
				try {
					Uninstall();
					return 0;
				} catch(Exception e) {
					Console.Error.WriteLine("Error uninstalling service: " + e.Message);
					return 1;
				}
			}

			// Check arguments that can be null,
			if (netConfig == null) {
				wout.WriteLine("Error, no network configuration given.");
				failed = true;
			} else if (nodeConfig == null) {
				wout.WriteLine("Error, no node configuration file given.");
				failed = true;
			}
			if (portArg == null) {
				wout.WriteLine("Error, no port address given.");
				failed = true;
			}

			wout.Flush();

			// If failed,
			if (failed) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.Width = Console.WindowWidth;
				formatter.CommandLineSyntax = "mnode";
				formatter.Options = options;
				formatter.PrintHelp();
				Console.Out.WriteLine();
				Console.Out.WriteLine(wout.ToString());
				return 1;
			}
			
			try {
				// Get the node configuration file,
				ConfigSource nodeConfigSource = new ConfigSource();
				using (FileStream fin = new FileStream(nodeConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					//TODO: make it configurable ...
					nodeConfigSource.LoadProperties(new BufferedStream(fin));
				}

				// Parse the network configuration string,
				NetworkConfigSource netConfigSource;
				using(FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					netConfigSource = new NetworkConfigSource();
					//TODO: make it configurable ...
					netConfigSource.LoadProperties(stream);
				}

				string password = nodeConfigSource.GetString("network_password", null);
				if (password == null) {
					Console.Out.WriteLine("Error: couldn't determine the network password.");
					return 1;
				}

				// configure the loggers
				Logger.Init(nodeConfigSource);

				//TODO: support also IPv6

				// The base path,
				IPAddress host = null;
				if (hostArg != null) {
					IPAddress[] addresses = Dns.GetHostAddresses(hostArg);
					for (int i = 0; i < addresses.Length; i++) {
						IPAddress address = addresses[i];
						if (address.AddressFamily == AddressFamily.InterNetwork) {
							host = address;
							break;
						}
					}
				} else {
					host = IPAddress.Loopback;
				}

				if (host == null) {
					Console.Out.WriteLine("Error: couldn't determine the host address.");
					return 1;
				}

				int port;
				if (!Int32.TryParse(portArg, out  port)) {
					Console.Out.WriteLine("Error: couldn't parse port argument: " + portArg);
					return 1;
				}
				
				string storage = commandLine.GetOptionValue("storage", null);
				IServiceFactory serviceFactory = GetServiceFactory(storage, nodeConfigSource);
				
				Console.Out.WriteLine("Machine Node, " + host + " : " + port);
				service = new TcpAdminService(serviceFactory, host, port, password);
				service.Config = netConfigSource;
				service.Start();

				waitHandle = new AutoResetEvent(false);
				waitHandle.WaitOne();
			} catch(Exception e) {
				Console.Out.WriteLine(e.Message);
				Console.Out.WriteLine(e.StackTrace);
				return 1;
			} finally {
				if (service != null)
					service.Dispose();
			}

			return 0;
		}

		private static void Install(string user, string password) {
			ServiceProcessInstaller processInstaller = new ServiceProcessInstaller();
			if (!String.IsNullOrEmpty(user)) {
				processInstaller.Account = ServiceAccount.NetworkService;
				processInstaller.Username = user;
				processInstaller.Password = password;
			} else {
				processInstaller.Account = ServiceAccount.NetworkService;
			}

			string execPath = Path.Combine(Environment.CurrentDirectory, "mnode.exe");
			string assemblyPath = String.Format("/assemblypath={0}", execPath);
			InstallContext context = new InstallContext(null, new string[] { assemblyPath });

			ServiceInstaller installer = new ServiceInstaller();
			installer.Context = context;
			installer.DisplayName = MachineNodeService.DisplayName;
			installer.Description = MachineNodeService.Description;
			installer.ServiceName = MachineNodeService.Name;
			installer.StartType = ServiceStartMode.Automatic;
			installer.Parent = processInstaller;

			ListDictionary state = new ListDictionary();
			installer.Install(state);
		}

		private static void Uninstall() {
			InstallContext context = new InstallContext(null, null);
			ServiceInstaller installer = new ServiceInstaller();
			installer.Context = context;
			installer.ServiceName = MachineNodeService.Name;
			ListDictionary state = new ListDictionary();
			installer.Uninstall(state);
		}
		
#if WIN32
		[DllImport("kernel32")]
		private static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);
		
		private delegate bool HandlerRoutine(CtrlTypes CtrlType);

		private static bool serviceDisposed;
		
		private enum CtrlTypes {
			CTRL_C_EVENT = 0,
			CTRL_BREAK_EVENT,
			CTRL_CLOSE_EVENT,
			CTRL_LOGOFF_EVENT = 5,
			CTRL_SHUTDOWN_EVENT
		}
		
		private static bool ConsoleCtrlCheck(CtrlTypes ctrlType) {
			try {
				if (service != null) {
					service.Stop();
					service = null;
					waitHandle.Set();
				}
			} catch(Exception e) {
				Console.Error.WriteLine("An error occurred while closing: " + e.Message);
				return false;
			}

			return true;
		}

#elif UNIX
		private static void CheckSignal() {
			Mono.Unix.UnixSignal[] signals = new Mono.Unix.UnixSignal[] {
				new Mono.Unix.UnixSignal(Mono.Unix.Native.Signum.SIGINT)
			};
			
			int index = Mono.Unix.UnixSignal.WaitAny(signals);
			Mono.Unix.Native.Signum signum = signals[index].Signum;
			if (signum == Mono.Unix.Native.Signum.SIGINT) {
				if (service != null) {
					service.Dispose();
					service = null;
				}
			}
		}
#endif
	}
}