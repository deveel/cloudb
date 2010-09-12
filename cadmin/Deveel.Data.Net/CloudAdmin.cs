using System;
using System.IO;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Shell;

namespace Deveel.Data.Net {
	[Option("netconfig", true, "Either a path or URL of the location of the network " +
							  "configuration file (default: network.conf).", IsRequired = true)]
	[Option("netpassword", true, "The challenge password used in all connection handshaking " +
							  "throughout the Mckoi network. All machines must have the " +
							  "same net password.", IsRequired = false)]
	public sealed class CloudAdmin : ShellApplication {

		protected override string Prompt {
			get { return "CloudB> "; }
		}

		protected override string About {
			get {
				StringWriter writer = new StringWriter();
				writer.WriteLine("---------------------------------------------------------------------------");
				writer.WriteLine(" {ApplicationName} {Version} {Copyright}");
				writer.WriteLine();
				writer.WriteLine(" CloudB Admin is provided AS IS and comes with ABSOLUTELY NO WARRANTY");
				writer.WriteLine(" This is free software, and you are welcome to redistribute it under the");
				writer.WriteLine(" conditions of the {License}");
				writer.WriteLine("---------------------------------------------------------------------------");
				return writer.ToString();
			}
		}

		protected override string License {
			get { return "Lesser GNU Public License <http://www.gnu.org/licenses/lgpl.txt>"; }
		}

		protected override bool Init(CommandLine commandLine) {
			if (!base.Init(commandLine))
				return false;

			Readline.WordBreakCharacters = new char[] { ' ' };
			return true;
		}

		protected override LineExecutionResultCode ExecuteLine(string line) {
			try {
				Execute(CurrentContext, line);
				return LineExecutionResultCode.Executed;
			} catch(Exception) {
				return LineExecutionResultCode.Empty;
			}
		}

		[STAThread]
		static void Main(string[] args) {
			try {
				int exitCode = Run(typeof(CloudAdmin), args);
				Environment.Exit(exitCode);
			} catch (Exception) {
				Environment.Exit(1);
			}
		}
	}
}