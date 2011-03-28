using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;

using Deveel.Configuration;

namespace Deveel.Data.Net.Client {
	partial class CloudBClientService : ServiceBase {
		private readonly CommandLine commandLine;
		private readonly EventLog eventLog;

		public CloudBClientService(CommandLine commandLine) {
			eventLog = new EventLog("CloudB Client Service", ".", "CloudB Client Service");

			this.commandLine = commandLine;

			InitializeComponent();
		}

		protected override void OnStart(string[] args) {
		}

		protected override void OnStop() {
		}

		public void Start(string[] args) {
			OnStart(args);
		}

	}
}
