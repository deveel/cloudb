using System;
using System.IO;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class AboutCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (!args.MoveNext()) {
				//TODO:
				StringWriter writer = new StringWriter();
				writer.WriteLine("---------------------------------------------------------------------------");
				writer.WriteLine(" {0} {1} {2}");
				writer.WriteLine();
				writer.WriteLine(" CloudB Admin is provided AS IS and comes with ABSOLUTELY NO WARRANTY");
				writer.WriteLine(" This is free software, and you are welcome to redistribute it under the");
				writer.WriteLine(" conditions of the {License}");
				writer.WriteLine("---------------------------------------------------------------------------");
				Out.Write(writer.ToString());
				return CommandResultCode.Success;
			}

			if (args.Current == "version") {
				//TODO:
			} else if (args.Current == "license") {
				Out.WriteLine("Lesser GNU Public License <http://www.gnu.org/licenses/lgpl.txt>");
				return CommandResultCode.Success;
			}

			return CommandResultCode.SyntaxError;
		}

		public override string Name {
			get { return "about"; }
		}

		public override string[] Synopsis {
			get { return new string[] { "about [ license | version ]" }; }
		}
	}
}