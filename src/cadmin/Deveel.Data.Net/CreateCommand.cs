using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class CreateCommand : Command {
		public override bool RequiresContext {
			get { return true; }
		}

		public override string[] Synopsis {
			get { return new string[] { "create table <name> with <column> ... [index <column> ...]" }; }
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			throw new NotImplementedException();
		}

		public override string Name {
			get { return "create"; }
		}
	}
}