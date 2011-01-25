using System;
using System.IO;
using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	class RefreshCommand : Command {
		public override string Name {
			get { return "refresh"; }
		}
		
		public override bool RequiresContext {
			get { return true; }
		}
		
		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (args.MoveNext())
				return CommandResultCode.SyntaxError;
			
			NetworkContext networkContext = context as NetworkContext;
			if (networkContext == null)
				return CommandResultCode.ExecutionFailed;
			
			Out.WriteLine();
			Out.WriteLine("refreshing...");
			Out.Flush();
			
			networkContext.Network.Refresh();
			
			try {
				//TODO:
				// networkContext.Network.Configuration.Reload();
			} catch (IOException e) {
				Error.WriteLine("Unable to refresh network config due to IO error");
				Error.WriteLine(e.Message);
				Error.WriteLine(e.StackTrace);
			}
			
			Out.WriteLine("done.");
			Out.WriteLine();
			
			return CommandResultCode.Success;
		}
	}
}