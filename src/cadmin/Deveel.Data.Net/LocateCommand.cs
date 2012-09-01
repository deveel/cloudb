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
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (!args.Current.Equals("path"))
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string pathName = args.Current;

			NetworkContext networkContext = (NetworkContext) context;
			PathInfo pathInfo = networkContext.Network.GetPathInfo(pathName);
			if (pathInfo == null) {
				Out.WriteLine("The path '" + pathName + "' was not found.");
				return CommandResultCode.ExecutionFailed;
			}

			IServiceAddress address = pathInfo.RootLeader;

			Out.Write("Root " + address);
			Out.WriteLine(" is managing path " + pathName);
			Out.Flush();
			return CommandResultCode.Success;
		}

		public override string Name {
			get { return "locate"; }
		}
	}
}