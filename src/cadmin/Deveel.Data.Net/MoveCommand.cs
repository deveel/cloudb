using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class MoveCommand : Command {
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			Error.WriteLine("Not Implemented");
			return CommandResultCode.ExecutionFailed;
		}

		public override string Name {
			get { return "move"; }
		}
	}
}