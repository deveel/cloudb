using System;
using System.IO;
using System.Threading;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

using NUnit.Framework;

namespace Deveel.Data.Net {
	[TestFixture]
	public abstract class NetworkTestBase {
		private NetworkProfile networkProfile;
		private AdminService adminService;
		private readonly StoreType storeType;
		private string path;

		private static readonly AutoResetEvent SetupEvent = new AutoResetEvent(true);

		protected NetworkTestBase(StoreType storeType) {
			this.storeType = storeType;
		}

		protected AdminService AdminService {
			get { return adminService; }
		}

		protected NetworkProfile NetworkProfile {
			get { return networkProfile; }
		}

		protected string TestPath {
			get {
				if (path == null)
					path = Path.Combine(Environment.CurrentDirectory, "base");
				return path;
			}
		}

		protected abstract IServiceAddress LocalAddress { get; }

		protected virtual void Config(ConfigSource config) {
			if (storeType == StoreType.FileSystem) {
				if (Directory.Exists(TestPath))
					Directory.Delete(TestPath, true);

				Directory.CreateDirectory(path);

				config.SetValue("node_directory", path);
			}

			config.SetValue("logger." + Logger.NetworkLoggerName + ".type", "simple-console");
			Logger.Init(config);
			Assert.IsInstanceOf(typeof(SimpleConsoleLogger), Logger.Network.BaseLogger);
		}

		protected abstract AdminService CreateAdminService(StoreType storeType);

		protected abstract IServiceConnector CreateConnector();

		[SetUp]
		public void SetUp() {
			SetupEvent.WaitOne();

			adminService = CreateAdminService(storeType);

			IServiceConnector connector = CreateConnector();

			NetworkConfigSource config = new NetworkConfigSource();
			Config(config);
			adminService.Config = config;
			adminService.Connector = connector;
			adminService.Start();
			networkProfile = new NetworkProfile(connector);

			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(LocalAddress);
			networkProfile.Configuration = netConfig;

			OnSetUp();

			SetupEvent.Set();
		}

		protected virtual void OnSetUp() {
		}

		[TearDown]
		public void TearDown() {
			try {
				OnTearDown();

				adminService.Stop();
				adminService.Dispose();

				if (storeType == StoreType.FileSystem &&
					Directory.Exists(TestPath))
					Directory.Delete(TestPath, true);
			} finally {
				SetupEvent.Set();
			}
		}

		protected virtual void OnTearDown() {
		}
	}
}