using System;

using Deveel.Commands;

namespace Deveel.Data.Net {
	[Command("start")]
	public sealed class StartRoleCommand : Command {
		public override CommandResultCode Execute(object context, string[] args) {
			return CommandResultCode.ExecutionFailed;
		}
	}
}