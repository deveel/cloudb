using System;
using System.IO;

using Deveel.Configuration;
using Deveel.Console;

namespace Deveel.Data.Net {
	public sealed class CloudAdmin : ShellApplication, IInterruptable {
		internal void SetNetworkContext(NetworkContext context) {
			SetActiveContext(context);
		}

		protected override void RegisterCommands() {
			Commands.Register(typeof(ConnectCommand));
			Commands.Register(typeof(DisconnectCommand));
			Commands.Register(typeof(ShowCommand));
			Commands.Register(typeof(RefreshCommand));
			
			Commands.Register(typeof(InitializeCommand));
			Commands.Register(typeof(DisposeCommand));
		}

		[STAThread]
		static void Main(string[] args) {
			CloudAdmin admin = new CloudAdmin();
			admin.SetPrompt("cloudb> ");
			admin.Interrupted += new EventHandler(CloudAdminInterrupted);

			Options options = admin.CreateOptions();
			ICommandLineParser parser = new GnuParser(options);
			CommandLine commandLine = null;

			try {
				commandLine = parser.Parse(args);
			} catch(Exception e) {
				System.Console.Error.WriteLine("Error while parsing arguments: {0}", e.Message);
				Environment.Exit(1);
			}
			
			admin.HandleCommandLine(commandLine);

			try {
				admin.Run();
			} catch(Exception) {
				admin.Shutdown();
			}
		}
		
		private static void CloudAdminInterrupted(object sender, EventArgs e) {
			CloudAdmin admin = (CloudAdmin)sender;
			admin.Exit(3);
		}
	}
}