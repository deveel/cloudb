using System;
using System.Net;
using System.Threading;

using Deveel.Data.Net.Client;
using Deveel.Data.Net.Security.Fake;

using NUnit.Framework;

namespace Deveel.Data.Net.Security {
	[TestFixture]
	public sealed class OAuthAuthenticatorTest {
		private FakeAdminService adminService;
		private RestPathClientService clientService;
		private FakeOAuthProvider provider;
		private OAuthAuthenticator authenticator;

		private Thread listenThread;
		private HttpListener httpListener;
		private bool listening;

		private const string PathName = "testdb";
		private const string PathTypeName = "Deveel.Data.BasePath, cloudbase";

		private const string ListenBaseAddress = "http://localhost:47756";
		private const string RequestTokenPath = "/request_token";
		private const string AccessTokenPath = "/access_token";
		private const string AuthorizationPath = "/authorize";
		private static readonly Uri AuthorizationAddress = new Uri(ListenBaseAddress + AuthorizationPath);
		private static readonly Uri RequestTokenAddress = new Uri(ListenBaseAddress + RequestTokenPath);
		private static readonly Uri AccessTokenAddress = new Uri(ListenBaseAddress + AccessTokenPath);
		private static readonly Uri PathServiceAddress = new Uri("http://localhost:2002");

		private static readonly FakeConsumer Consumer = new FakeConsumer("antonello", "123456", ConsumerStatus.Valid);

		private void Authorize(HttpListenerContext context) {
			HttpListenerRequest request = context.Request;
		}

		private void StartListner() {
			httpListener = new HttpListener();
			httpListener.Prefixes.Add(new Uri(ListenBaseAddress).ToString());
			httpListener.Start();
			listening = true;

			listenThread = new Thread(Listen);
			listenThread.Start();
			listenThread.IsBackground = true;
		}

		private void Listen() {
			while (httpListener.IsListening) {
				if (!listening)
					return;
				
				HttpListenerContext context;
				try {
					context = httpListener.GetContext();
				} catch (HttpListenerException e) {
					Console.Error.WriteLine(e.Message);
					Console.Error.WriteLine(e.StackTrace);
					continue;
				}
				
				string rawUrl = context.Request.RawUrl;

				Console.Out.WriteLine("Processing call to {0}", rawUrl);

				TokenIssueResult result;
				if (rawUrl == RequestTokenPath) {
					result = provider.IssueToken(TokenType.Request, context);
				} else if (rawUrl == AccessTokenPath) {
					result = provider.IssueToken(TokenType.Access, context);
				} else if (rawUrl == AuthorizationPath) {
					Authorize(context);
					result = new TokenIssueResult(true);
				} else {
					continue;
				}

				if (result == null || !result.Success) {
					context.Response.StatusCode = (int) HttpStatusCode.BadRequest;
				} else {
					context.Response.StatusCode = (int) HttpStatusCode.OK;
				}

				context.Response.Close();
			}
		}

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
			HeapConsumerStore consumerStore = new HeapConsumerStore();
			consumerStore.Add(Consumer);

			provider = new FakeOAuthProvider();
			provider.TokenStore = tokenStore;
			provider.ConsumerStore = consumerStore;
			provider.TokenGenerator = new GuidTokenGenerator();
			provider.RequestIdValidator = new HeapRequestIdValidator(20);
			provider.VerificationProvider = new MD5HashVerificationProvider();

			authenticator = new OAuthAuthenticator();

			clientService = new RestPathClientService(new HttpServiceAddress(PathServiceAddress), FakeServiceAddress.Local,
			                                          new FakeServiceConnector(adminService));
			clientService.Authenticator = authenticator;
			clientService.Init();

			StartListner();
		}

		[TearDown]
		public void TearDown() {
			listening = false;

			if (httpListener != null && httpListener.IsListening)
				httpListener.Abort();

			if (clientService != null && clientService.IsConnected)
				clientService.Dispose();
			if (adminService != null)
				adminService.Dispose();
		}

		[Test]
		public void TestSuccess1() {
			OAuthService service = new OAuthService(new OAuthEndPoint(RequestTokenAddress), AuthorizationAddress, new OAuthEndPoint(AccessTokenAddress), Consumer);
			OAuthClientContext clientContext = new OAuthClientContext(service);
			clientContext.NonceGenerator = new GuidNonceGenerator();

			UriBuilder builder = new UriBuilder();
			builder.Scheme = Uri.UriSchemeHttp;
			builder.Host = PathServiceAddress.Host;
			builder.Port = PathServiceAddress.Port;
			builder.Path = PathName;

			OAuthRequest consumerRequest = clientContext.CreateRequest(new OAuthEndPoint(builder.ToString(), "GET"));
			OAuthResponse resource = consumerRequest.GetResource();

			if (resource.HasProtectedResource) {
				//TODO:
			}
		}
	}
}