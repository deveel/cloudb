using System;

using Deveel.Console;
using Deveel.Console.Commands;

namespace Deveel.Data.Net {
	internal class RollbackCommand : Command {
		public override string Name {
			get { return "rollback"; }
		}

		public override string[] Synopsis {
			get { return new string[] { "rollback path <name> to <date>" }; }
		}

		public override bool RequiresContext {
			get { return true; }
		}

		private CommandResultCode RollbackPathToTime(NetworkContext context, string pathName, DateTime time) {
			PathInfo pathInfo = context.Network.GetPathInfo(pathName);
			if (pathInfo == null) {
				Out.WriteLine("The path '" + pathName + "' was not found.");
				return CommandResultCode.ExecutionFailed;
			}

			IServiceAddress address = pathInfo.RootLeader;

			Out.WriteLine("Reverting path " + pathName + " to " + time);

			DataAddress[] dataAddresses = context.Network.GetHistoricalPathRoots(address, pathName, time, 20);

			if (dataAddresses.Length == 0) {
				Out.WriteLine("no historical roots found.");
				return CommandResultCode.ExecutionFailed;
			}

			Out.WriteLine();
			Out.WriteLine("found the following roots:");
			foreach (DataAddress da in dataAddresses) {
				Out.WriteLine("  " + da);
			}

			Out.WriteLine();
			Out.WriteLine("WARNING: Great care must be taken when rolling back a path. This");
			Out.WriteLine(" operation is only intended as a way to recover from some types");
			Out.WriteLine(" of corruption or other data inconsistency issues.");
			Out.WriteLine(" Before agreeing to rollback the path, ensure there are no open");
			Out.WriteLine(" writable transactions currently active on the path. A commit");
			Out.WriteLine(" write on this path before this operation completes may undo the");
			Out.WriteLine(" rollback or worse, put the path back into an inconsistent ");
			Out.WriteLine(" state.");
			Out.WriteLine();

			Question question = new Question("If you are sure you want to continue type YES (case-sensitive) ",
			                                 new object[] {"YES", "no"}, 1);
			Answer answer = question.Ask(Application, false);
			if (answer.SelectedOption != 0)
				return CommandResultCode.ExecutionFailed;

			Out.WriteLine();

			context.Network.SetPathRoot(address, pathName, dataAddresses[0]);
			Out.WriteLine("done.");
			return CommandResultCode.Success;
		}

		private CommandResultCode Rollback(NetworkContext networkContext, string pathName, string time, bool hours) {
			if (hours) {
				int numHours;
				if (!Int32.TryParse(time, out numHours)) {
					Error.WriteLine("must be a valid number of hours.");
					return CommandResultCode.SyntaxError;
				}

				Out.WriteLine("reverting " + time + " hours.");

				DateTime timems = DateTime.Now.AddHours(-numHours);
				RollbackPathToTime(networkContext, pathName, timems);
			} else {
				// Try and parse it
				try {
					DateTime date = DateTime.Parse(time);

					return RollbackPathToTime(networkContext, pathName, date);
				} catch (FormatException) {
					Error.Write("unable to parse timestamp '");
					Error.Write(time);
					Error.Write("': ");
					Error.Write("must be formatted in one of the standards formats ");
					Error.WriteLine("(eg. 'feb 25, 2010 2:25am')");
					return CommandResultCode.SyntaxError;
				}
			}

			return CommandResultCode.ExecutionFailed;
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			NetworkContext networkContext = (NetworkContext) context;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "path")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string pathName = args.Current;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (args.Current != "to")
				return CommandResultCode.SyntaxError;
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string time = args.Current;

			bool hours = false;

			if (args.MoveNext()) {
				if (args.Current != "hours")
					return CommandResultCode.SyntaxError;

				hours = true;
			}

			try {
				return Rollback(networkContext, pathName, time, hours);
			} catch(Exception) {
				Error.WriteLine("unable to rollback the path to the given date.");
				Error.WriteLine();
				return CommandResultCode.ExecutionFailed;
			}
		}
	}
}