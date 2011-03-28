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

namespace Deveel.Data.Net.Client {
	public static class PathClient {
		private static PathClientService service;

#if WINDOWS
		private static HandlerRoutine ExitCallback;
#endif

		private static AutoResetEvent waitHandle;

		private static Options GetOptions() {
			Options options = new Options();
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
			options.AddOption("protocol", true, "The connection protocol used by this node to listen connections");
			return options;
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

		public static int Main(string[] args) {
			string netConfig = null;
			string hostArg = null, portArg = null;

			StringWriter wout = new StringWriter();
			Options options = GetOptions();

			CommandLine commandLine = null;

			bool failed = false;
			bool isService = false;

			try {
				ICommandLineParser parser = new GnuParser(options);
				commandLine = parser.Parse(args);

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
				CloudBClientService clientService = new CloudBClientService(commandLine);

				try {
					if (Environment.UserInteractive) {
						clientService.Start(args);
						Console.Out.WriteLine("Press any key to stop...");
						Console.Read();
						clientService.Stop();
					} else {
						ServiceBase.Run(clientService);
					}
				} catch (Exception) {
					return 1;
				}

				return 0;
			}

			AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(OnUnhandledException);
			SetEventHandlers();

			ProductInfo libInfo = ProductInfo.GetProductInfo(typeof(PathClientService));
			ProductInfo nodeInfo = ProductInfo.GetProductInfo(typeof(PathClient));

			Console.Out.WriteLine("{0} {1} ( {2} )", nodeInfo.Title, nodeInfo.Version, nodeInfo.Copyright);
			Console.Out.WriteLine(nodeInfo.Description);
			Console.Out.WriteLine();
			Console.Out.WriteLine("{0} {1} ( {2} )", libInfo.Title, libInfo.Version, libInfo.Copyright);

			// Check arguments that can be null,
			if (netConfig == null) {
				wout.WriteLine("Error, no network configuration given.");
				failed = true;
			}

			if (portArg == null) {
				wout.WriteLine("Error, no port address given.");
				failed = true;
			}

			if (!failed) {
				//TODO: support for remote (eg. HTTP, FTP, TCP/IP) configurations)

				netConfig = NormalizeFilePath(netConfig);

				if (!File.Exists(netConfig)) {
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
				Console.Out.WriteLine("Retrieving network configuration from {0}", netConfig);
#endif

				// Parse the network configuration string,
				NetworkConfigSource netConfigSource;
				using (FileStream stream = new FileStream(netConfig, FileMode.Open, FileAccess.Read, FileShare.None)) {
					netConfigSource = new NetworkConfigSource();
					//TODO: make it configurable ...
					netConfigSource.LoadProperties(stream);
				}

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

				Console.Out.WriteLine("Path Client Service, " + host + " : " + port);
				//TODO:
				service = new TcpPathClientService(null, null, null);
				service.Init();

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

		private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e) {
			Console.Error.WriteLine("Unhandled exception: {0}", e.ExceptionObject);
			Environment.Exit(1);
		}

		private static void Uninstall() {
			throw new NotImplementedException();
		}

		private static void Install(CommandLine commandLine) {
			throw new NotImplementedException();
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