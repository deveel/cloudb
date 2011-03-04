using System;
using Deveel.Data.Net.Client;
using Deveel.Data.Net.Security.Fake;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public sealed class OAuthAuthenticatorTest {
		private FakeAdminService adminService;
		private RestPathClientService clientService;
		private OAuthAuthenticator authenticator;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";

		private static readonly FakeConsumer Consumer = new FakeConsumer("antonello", "123456", ConsumerStatus.Valid);
		private static readonly OAuthToken RequestToken = new OAuthToken(TokenType.Request, "33FRT567", "345434", Consumer);

		[SetUp]
		public void SetUp() {
			adminService = new FakeAdminService(NetworkStoreType.Memory);
			adminService.Start();

			NetworkProfile networkProfile = new NetworkProfile(new FakeServiceConnector(adminService));
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(FakeServiceAddress.Local);
			networkProfile.Configuration = netConfig;
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Manager);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Root);
			networkProfile.RegisterRoot(FakeServiceAddress.Local);
			networkProfile.StartService(FakeServiceAddress.Local, ServiceType.Block);
			networkProfile.RegisterBlock(FakeServiceAddress.Local);
			networkProfile.Refresh();

			networkProfile.AddPath(FakeServiceAddress.Local, PathName, PathTypeName);
			networkProfile.Refresh();

			HeapTokenStore tokenStore = new HeapTokenStore();
			tokenStore.Add(RequestToken);
			HeapConsumerStore consumerStore = new HeapConsumerStore();
			consumerStore.Add(Consumer);

			authenticator = new OAuthAuthenticator();
			authenticator.TokenStore = tokenStore;
			authenticator.ConsumerStore = consumerStore;
			authenticator.TokenGenerator = new GuidTokenGenerator();
			authenticator.RequestIdValidator = new HeapRequestIdValidator(20);
			authenticator.VerificationProvider = new MD5HashVerificationProvider();

			clientService = new RestPathClientService(new HttpServiceAddress("localhost", 2002), FakeServiceAddress.Local,
			                                          new FakeServiceConnector(adminService));
			clientService.Authenticator = authenticator;
			clientService.Init();
		}

		[TearDown]
		public void TearDown() {
			if (clientService != null && clientService.IsConnected)
				clientService.Dispose();
			if (adminService != null)
				adminService.Dispose();
		}

		[Test]
		public void TestSuccess1() {
			OAuthService service = new OAuthService(new OAuthEndPoint("http://localhost:2003"), new Uri("http://localhost:6578"), new OAuthEndPoint("http://localhost:2002"), Consumer);
			OAuthClientContext clientContext = new OAuthClientContext(service);
			OAuthRequest consumerRequest = clientContext.CreateRequest(new OAuthEndPoint("http://localhost:2002/testdb", "GET"));
			consumerRequest.GetResource();
		}
	}
}