using System;
using System.ServiceProcess;

namespace Deveel.Data.Net {
	internal class MachineNodeService : ServiceBase {
		public const string DisplayName = "CloudB Machine Node";
		public const string Name = "mnode";
		public const string Description = "The system service that makes this machine one of the nodes of a CloudB network.";

		public MachineNodeService() {
			ServiceName = Name;
			AutoLog = true;
		}

		protected override void OnStart(string[] args) {
			base.OnStart(args);
		}

		protected override void OnStop() {
			base.OnStop();
		}

		protected override void OnShutdown() {
			base.OnShutdown();
		}

		protected override bool OnPowerEvent(PowerBroadcastStatus powerStatus) {
			return base.OnPowerEvent(powerStatus);
		}

		protected override void OnPause() {
			base.OnPause();
		}

		protected override void OnSessionChange(SessionChangeDescription changeDescription) {
			base.OnSessionChange(changeDescription);
		}
	}
}