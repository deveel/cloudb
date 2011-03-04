using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class LocateCommand : Command {
		public override string[] Synopsis {
			get { return new string[] { "locate path <name>" }; }
		}

		public override bool RequiresContext {
			get { return true; }
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			throw new NotImplementedException();
		}

		public override string Name {
			get { return "locate"; }
		}
	}
}