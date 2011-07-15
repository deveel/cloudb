using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

#if UNIX
using Mono.Unix.Native;
#endif
using System.ServiceProcess;
using System.Text;
using System.Threading;

using Deveel.Configuration;
using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Util;

using Microsoft.Win32;

namespace Deveel.Data.Net {
	public static class MachineNode {
		private static TcpAdminService service;

#if WINDOWS
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
			options.AddOption("service", false, "Starts the node as a service (used internally)");
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
#if WINDOWS
			ExitCallback = new HandlerRoutine(ConsoleCtrlCheck);
			SetConsoleCtrlHandler(ExitCallback, true);
#elif UNIX			
			Thread signalThread = new Thread(CheckSignal);
			signalThread.Start();
#endif
		}

		private static string NormalizeFilePath(string fileName) {
			if (String.IsNullOrEmpty(fileName))
				return null;

			if (fileName[0] == '.')
				fileName = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName));

			return fileName;
		}

		[STAThread]
		private static int Main(string[] args) {
			string nodeConfig = null, netConfig = null;
			string hostArg = null, portArg = null;

			StringWriter wout = new StringWriter();
			Options options = GetOptions();

			CommandLine commandLine = null;

			bool failed = false;
			bool isService = false;

			try {
				ICommandLineParser parser = new GnuParser(options);
				commandLine = parser.Parse(args);

				nodeConfig = commandLine.GetOptionValue("nodeconfig", "./node.conf");
				netConfig = commandLine.GetOptionValue("netconfig", "./network.conf");
				hostArg = commandLine.GetOptionValue("host");
				portArg = commandLine.GetOptionValue("port");
			} catch (ParseException) {
				wout.WriteLine("Error parsing arguments.");
				failed = true;
			}

			if (commandLine != null) {
				if (commandLine.HasOption("install")) {
					try {
						Install(commandLine);
						Console.Out.WriteLine("Service installed succesfully.");
						return 0;
					} catch (Exception e) {
						Console.Error.WriteLine("Error installing service: " + e.Message);
#if DEBUG
						Console.Error.WriteLine(e.StackTrace);
#endif
						return 1;
					}
				}
				if (commandLine.HasOption("uninstall")) {
					try {
						Uninstall();
						Console.Out.WriteLine("Service uninstalled succesfully.");
						return 0;
					} catch (Exception e) {
						Console.Error.WriteLine("Error uninstalling service: " + e.Message);
#if DEBUG
						Console.Error.WriteLine(e.StackTrace);
#endif
						return 1;
					}
				}

				isService = commandLine.HasOption("service");
			}

			if (isService) {
				MachineNodeService mnodeService = new MachineNodeService(commandLine);

				try {
					if (Environment.UserInteractive) {
						mnodeService.Start(args);
						Console.Out.WriteLine("Press any key to stop...");
						Console.Read();
						mnodeService.Stop();
					} else {
						ServiceBase.Run(mnodeService);
					}
				} catch(Exception) {
					return 1;
				}

				return 0;
			}

			AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(OnUnhandledException);
			SetEventHandlers();

			ProductInfo libInfo = ProductInfo.GetProductInfo(typeof (TcpAdminService));
			ProductInfo nodeInfo = ProductInfo.GetProductInfo(typeof (MachineNode));

			Console.Out.WriteLine("{0} {1} ( {2} )", nodeInfo.Title, nodeInfo.Version, nodeInfo.Copyright);
			Console.Out.WriteLine(nodeInfo.Description);
			Console.Out.WriteLine();
			Console.Out.WriteLine("{0} {1} ( {2} )", libInfo.Title, libInfo.Version, libInfo.Copyright);

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

			if (!failed) {
				//TODO: support for remote (eg. HTTP, FTP, TCP/IP) configurations)

				nodeConfig = NormalizeFilePath(nodeConfig);
				netConfig = NormalizeFilePath(netConfig);

				if (!File.Exists(nodeConfig)) {
					wout.WriteLine("Error, node configuration file not found ({0}).", nodeConfig);
					failed = true;
				} else if (!File.Exists(netConfig)) {
					wout.WriteLine("Error, node configuration file not found ({0}).", netConfig);
					failed = true;
				}
			}

			wout.Flush();

			// If failed,
			if (failed) {
				HelpFormatter formatter = new HelpFormatter();
				if (!IsConsoleRedirected()) {
					formatter.Width = Console.WindowWidth;
				}
				formatter.CommandLineSyntax = "mnode";
				formatter.Options = options;
				formatter.PrintHelp();
				Console.Out.WriteLine();
				Console.Out.WriteLine(wout.ToString());
				return 1;
			}

			try {
#if DEBUG
				Console.Out.WriteLine("Retrieving node configuration from {0}", nodeConfig);
#endif

				// Get the node configuration file,
				ConfigSource nodeConfigSource = new ConfigSource();
				using (FileStream fin = new FileStream(nodeConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					//TODO: make it configurable ...
					nodeConfigSource.LoadProperties(new BufferedStream(fin));
				}

#if DEBUG
				Console.Out.WriteLine("Retrieving network configuration from {0}", netConfig);
#endif

				// Parse the network configuration string,
				NetworkConfigSource netConfigSource;
				using (FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
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
				if (!Int32.TryParse(portArg, out port)) {
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
			} catch (Exception e) {
				Console.Out.WriteLine(e.Message);
				Console.Out.WriteLine(e.StackTrace);
				return 1;
			} finally {
				if (service != null)
					service.Dispose();
			}

			return 0;
		}

		private static bool IsConsoleRedirected() {
			try {
				int dummy = Console.WindowWidth;
				return false;
			} catch (IOException) {
				return true;
			}
		}

		static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e) {
			Console.Error.WriteLine("Unhandled exception: {0}", e.ExceptionObject);
			Environment.Exit(1);
		}

		private static string GetServiceCommandLine(CommandLine commandLine) {
			bool nodeConfigFound = false, netConfigFound = false;

			Option[] options = commandLine.Options;
			List<Option> normOptions = new List<Option>(options.Length);
			for (int i = 0; i < options.Length; i++) {
				Option opt = options[i];
				if ((opt.Name == "user" || opt.LongName == "user") ||
					(opt.Name == "user" || opt.LongName == "password") ||
					(opt.Name == "service" || opt.LongName == "service") ||
					(opt.Name == "install" || opt.LongName == "install"))
					continue;

				if (opt.Name == "nodeconfig" || opt.LongName == "nodeconfig") {
					nodeConfigFound = true;
				} else if (opt.Name == "netconfig" || opt.LongName == "netconfig") {
					netConfigFound = true;
				}

				normOptions.Add(opt);
			}

			if (!nodeConfigFound)
				normOptions.Add(new Option("nodeconfig", true, ""));
			if (!netConfigFound)
				normOptions.Add(new Option("netconfig", true, ""));

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < normOptions.Count; i++) {
				Option opt = normOptions[i];
				sb.Append("-");
				if (opt.HasLongName) {
					sb.Append("-");
					sb.Append(opt.LongName);
				} else {
					sb.Append(opt.Name);
				}

				if (opt.HasArgument) {
					sb.Append(" ");

					string value;
					if (opt.Name == "netconfig") {
						value = NormalizeFilePath(commandLine.GetOptionValue(opt.Name, "./network.conf"));
					} else if (opt.Name == "nodeconfig") {
						value = NormalizeFilePath(commandLine.GetOptionValue(opt.Name, "./node.conf"));
					} else {
						value = commandLine.GetOptionValue(opt.Name);
					}

					sb.Append(value);
				}

				if (i < normOptions.Count - 1)
					sb.Append(" ");
			}

			return sb.ToString();
		}

		private static void Install(CommandLine commandLine) {
			string options = GetServiceCommandLine(commandLine);

			string logFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "install.log");
			string assemblyPath = typeof (MachineNode).Assembly.Location;
			InstallContext context = new InstallContext(logFile,
			                                            new string[] {
			                                                         	String.Format("/assemblypath={0}", assemblyPath),
			                                                         	String.Format("/logfile={0}", logFile)
			                                                         });
			if (!String.IsNullOrEmpty(options))
				context.Parameters["AdditionalOptions"] = options;

			ListDictionary savedState = new ListDictionary();

			AssemblyInstaller installer = new AssemblyInstaller(typeof (MachineNodeService).Assembly, new string[0]);
			installer.Context = context;
			installer.UseNewContext = false;
			installer.AfterInstall += AfterInstall;
			installer.Install(savedState);
			installer.Commit(savedState);
		}

		private static void AfterInstall(object sender, InstallEventArgs e) {
			AssemblyInstaller installer = (AssemblyInstaller)sender;
#if WINDOWS
			RegistryKey system = Registry.LocalMachine.OpenSubKey("System");
			RegistryKey currentControlSet = system.OpenSubKey("CurrentControlSet");
			RegistryKey servicesKey = currentControlSet.OpenSubKey("Services");
			RegistryKey serviceKey = servicesKey.OpenSubKey(MachineNodeService.Name, true);

			string options = null;
			if (installer.Context.Parameters.ContainsKey("AdditionalOptions"))
				options = installer.Context.Parameters["AdditionalOptions"];

			StringBuilder sb = new StringBuilder((string)serviceKey.GetValue("ImagePath"));
			sb.Append(" ");
			sb.Append("-service");
			if (!String.IsNullOrEmpty(options)) {
				sb.Append(" ");
				sb.Append(options);
			}

			serviceKey.SetValue("ImagePath", sb.ToString());
#endif
		}

		private static void Uninstall() {
			string logFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "uninstall.log");
			InstallContext context = new InstallContext(logFile, new string[] {String.Format("/logfile={0}", logFile)});
			AssemblyInstaller installer = new AssemblyInstaller(typeof (MachineNodeService).Assembly,
			                                                    new string[] {String.Format("/logfile={0}", logFile)});
			installer.Context = context;
			installer.UseNewContext = false;
			installer.Uninstall(null);
			installer.Commit(null);
		}

#if WINDOWS
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