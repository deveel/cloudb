using System;
using System.IO;

using Deveel.Configuration;
using Deveel.Console;

namespace Deveel.Data.Net {
	public sealed class CloudAdmin : ShellApplication {
		protected override Options CreateOptions() {
			Options options = new Options();
			Option option = new Option("netconfig", true, "Either a path or URL of the location of the network " +
			                                              "configuration file (default: network.conf).");
			option.IsRequired = true;
			options.AddOption(option);
			return options;
		}

		internal void SetNetworkContext(NetworkContext context) {
			SetActiveContext(context);
		}


		[STAThread]
		static void Main(string[] args) {
			CloudAdmin admin = new CloudAdmin();

			Options options = admin.CreateOptions();
			ICommandLineParser parser = new GnuParser(options);
			CommandLine commandLine = null;

			try {
				commandLine = parser.Parse(args);
			} catch(Exception e) {
				System.Console.Error.WriteLine("Error while parsing arguments: {0}", e.Message);
				Environment.Exit(1);
			}

			string networkConf = commandLine.GetOptionValue("netconfig", "network.conf");
			string networkPass = commandLine.GetOptionValue("netpassword");

			bool failed = false;

			// Check arguments that can be null,
			if (networkConf == null) {
				System.Console.Error.WriteLine("Error, no network configuration file/url given.");
				failed = true;
			}
			if (networkPass == null) {
				System.Console.Error.WriteLine("Error, no network password given.");
				failed = true;
			}

			if (failed) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.Width = System.Console.WindowWidth;
				formatter.CommandLineSyntax = "cadmin";
				formatter.Options = options;
				formatter.PrintHelp(System.Console.Out, true);
				Environment.Exit(0);
			}

			admin.HandleCommandLine(commandLine);

			try {
				NetworkProfile networkProfile = new NetworkProfile();
				admin.SetActiveContext(new NetworkContext());
				admin.Run();
			} catch(Exception) {
				admin.Shutdown();
			}
		}
	}
}