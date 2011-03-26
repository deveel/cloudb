using System;
using System.ComponentModel;
using System.Configuration.Install;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;

using Microsoft.Win32;

namespace Deveel.Data.Net {
	[RunInstaller(true)]
	public partial class ProjectInstaller : Installer {
		public ProjectInstaller() {
			InitializeComponent();
		}

		private static EventLogInstaller FindInstaller(InstallerCollection installers) {
			foreach (Installer installer in installers) {
				if (installer is EventLogInstaller) {
					return (EventLogInstaller)installer;
				}

				EventLogInstaller eventLogInstaller = FindInstaller(installer.Installers);
				if (eventLogInstaller != null) {
					return eventLogInstaller;
				}
			}
			return null;
		}


		private void serviceProcessInstaller_BeforeInstall(object sender, InstallEventArgs e) {
			Installer installer = (Installer) sender;
			if (installer.Context.Parameters.ContainsKey("User")) {
				serviceProcessInstaller.Username = installer.Context.Parameters["User"];
				serviceProcessInstaller.Password = installer.Context.Parameters["Password"];
				serviceProcessInstaller.Account = ServiceAccount.User;
			}
		}

		private void serviceInstaller_BeforeInstall(object sender, InstallEventArgs e) {
			EventLogInstaller logInstaller = FindInstaller(serviceInstaller.Installers);
			if (logInstaller != null)
				logInstaller.Log = "CloudB Machine Node";

			serviceInstaller.DisplayName = MachineNodeService.DisplayName;
			serviceInstaller.ServiceName = MachineNodeService.Name;
			serviceInstaller.Description = MachineNodeService.Description;
		}
	}
}
