using System;
using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class DisconnectCommand : Command {
		public override string Name {
			get { return "disconnect"; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (context == null)
				return CommandResultCode.ExecutionFailed;
			
			if (args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			((CloudAdmin)Application).SetNetworkContext(null);
			
			Out.WriteLine("successfully disconnected...");
			Out.WriteLine();
			return CommandResultCode.Success;
		}
	}
}