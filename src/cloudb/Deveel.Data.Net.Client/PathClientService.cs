using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Security;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net.Client {
	public abstract class PathClientService : Component {
		protected PathClientService(IServiceAddress address, IServiceAddress managerAddress, IServiceConnector connector) {
			this.address = address;
			this.managerAddress = managerAddress;
			this.connector = connector;
			
			NetworkConfigSource netConfig = new NetworkConfigSource();
			netConfig.AddNetworkNode(managerAddress);

			network = new NetworkProfile(connector);
			network.Configuration = netConfig;

			log = Logger.Network;
		}

		~PathClientService() {
			Dispose(false);
		}

		private readonly IServiceAddress address;
		private readonly IServiceAddress managerAddress;
		private readonly IServiceConnector connector;
		private IMessageSerializer messageSerializer;
		private readonly Logger log;

		private IAuthenticator authenticator;

		private NetworkClient client;
		private readonly NetworkProfile network;
		private string transactionIdKey;

		private readonly Dictionary<string, string> pathTypes = new Dictionary<string, string>();
		private readonly List<HandlerContainer> handlers = new List<HandlerContainer>();

		protected IServiceConnector Connector {
			get { return connector; }
		}

		protected IServiceAddress ManagerAddress {
			get { return managerAddress; }
		}

		protected IServiceAddress Address {
			get { return address; }
		}

		protected NetworkClient Client {
			get { return client; }
		}

		protected abstract string Type { get; }

		protected Logger Logger {
			get { return log; }
		}

		public virtual IMessageSerializer MessageSerializer {
			get {
				if (messageSerializer == null)
					messageSerializer = new BinaryRpcMessageSerializer();
				return messageSerializer;
			}
			set { messageSerializer = value; }
		}

		public IAuthenticator Authenticator {
			get { return authenticator; }
			set { authenticator = value; }
		}

		public bool IsConnected {
			get { return client != null && client.IsConnected; }
		}

		public string TransactionIdKey {
			get { return transactionIdKey; }
			set { transactionIdKey = value; }
		}

		private void ScanForHandlers() {
			if (handlers.Count != 0)
				return;

			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Type[] types = assemblies[i].GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (type != typeof(IRequestHandler) &&
					    typeof(IRequestHandler).IsAssignableFrom(type) &&
					    !type.IsAbstract) {
						HandleAttribute handle = Attribute.GetCustomAttribute(type, typeof(HandleAttribute)) as HandleAttribute;
						if (handle == null)
							continue;

						handlers.Add(new HandlerContainer(this, handle.PathTypeName, type));
					}
				}
			}
		}

		private void GetPathProfiles() {
			network.Refresh();
			
			PathProfile[] profiles = network.GetPaths();
			for (int i = 0; i < profiles.Length; i++) {
				PathProfile path = profiles[i];
				pathTypes[path.Path] = path.PathType;
			}
		}

		private HandlerContainer DoGetMethodHandler(string pathName, int tryCount) {
			string pathTypeName;
			if (!pathTypes.TryGetValue(pathName, out pathTypeName)) {
				if (tryCount == 0) {
					GetPathProfiles();
					return DoGetMethodHandler(pathName, tryCount + 1);
				}

				return null;
			}

			for (int i = 0; i < handlers.Count; i++) {
				HandlerContainer container = handlers[i];
				if (container.PathTypeName == pathTypeName)
					return container;
			}

			return null;
		}

		private HandlerContainer GetMethodHandler(string pathName) {
			return DoGetMethodHandler(pathName, 0);
		}

		protected virtual void OnInit() {
		}

		private ClientRequestMessage GetMethodRequest(RequestType type, PathTransaction transaction, Stream requestStream) {
			ClientRequestMessage request = new ClientRequestMessage(Type, type, transaction.Transaction);
			if (requestStream != null) {
				Message streamedRequest = MessageSerializer.Deserialize(requestStream, MessageType.Request);
				foreach(KeyValuePair<string, object> attribute in streamedRequest.Attributes) {
					request.Attributes.Add(attribute.Key, attribute.Value);
				}
				foreach(MessageArgument argument in streamedRequest.Arguments) {
					request.Arguments.Add(argument);
				}
			}
			return request;
		}

		protected ResponseMessage HandleRequest(object context, RequestType type, string pathName, IDictionary<string, PathValue> args, Stream requestStream) {
			//TODO: allow having multiple handlers for the service ...
			HandlerContainer handler = GetMethodHandler(pathName);
			if (handler == null)
				throw new InvalidOperationException("No handler was found for the path '" + pathName + "' in this context.");

			if (!handler.Handler.CanHandleClientType(Type))
				throw new InvalidOperationException("The handler for the path '" + pathName + "' cannot support client of type '" + Type + "'.");

			IPathTransaction transaction;

			if (TransactionIdKey != null && (args != null && args.ContainsKey(TransactionIdKey))) {
				int tid = args[TransactionIdKey].ToInt32();
				transaction = GetTransaction(pathName, tid);
			} else {
				transaction = CreateTransaction(pathName);
			}

			ClientRequestMessage request = GetMethodRequest(type, ((PathTransaction) transaction), requestStream);
			if (args != null) {
				foreach(KeyValuePair<string, PathValue> pair in args) {
					request.Attributes.Add(pair.Key, pair.Value);
				}
			}
			request.Seal();

			if (authenticator != null) {
				AuthRequest authRequest = new AuthRequest(context, pathName);
				foreach (KeyValuePair<string, object> pair in request.Attributes)
					authRequest.AuthData.Add(pair.Key, new AuthObject(pair.Value));

				AuthResult authResult = authenticator.Authenticate(authRequest);
				if (authResult != null) {
					if (!authResult.Success) {
						Logger.Info(authenticator, String.Format("Unauthorized: {0} ({1})", authResult.Message, authResult.Code));

						ResponseMessage responseMessage = request.CreateResponse("error");
						responseMessage.Code = MessageResponseCode.Unauthorized;
						//TODO: Extend MessageError to include an error specific code ...
						responseMessage.Arguments.Add(new MessageError(authResult.Message));
						return responseMessage;
					}
						
					Logger.Info(authenticator, String.Format("Authorized: {0}", authResult.Message));
				}
			}

			return handler.Handler.HandleRequest(request);
		}
		
		protected ResponseMessage HandleRequest(object context, RequestType type, string pathName, Stream requestStream) {
			return HandleRequest(context, type, pathName, null, requestStream);
		}

		protected IPathTransaction CreateTransaction(string pathName) {
			HandlerContainer container = GetMethodHandler(pathName);
			if (container == null)
				throw new InvalidOperationException();

			return container.CreateTransaction(pathName);
		}

		protected IPathTransaction GetTransaction(string pathName, int id) {
			HandlerContainer container = GetMethodHandler(pathName);
			if (container == null)
				throw new ArgumentException();
			return container.GetTransaction(id);
		}

		protected virtual void OnTransactionCommitted(string pathName, IPathTransaction transaction, DataAddress dataAddress) {
		}

		protected virtual void OnTransactionDisposed(string pathName, IPathTransaction transaction) {
		}

		public void Init() {
			ScanForHandlers();

			client = new NetworkClient(managerAddress, connector);
			client.Connect();

			OnInit();
		}

		#region HandlerContainer

		private class HandlerContainer {
			private readonly PathClientService service;
			private readonly string pathTypeName;
			private readonly Type handlerType;
			private IRequestHandler handler;
			private readonly Dictionary<string, IPathContext> contexts;

			private int connId = -1;
			private readonly Dictionary<int, PathTransaction> transactions = new Dictionary<int, PathTransaction>();

			public HandlerContainer(PathClientService service, string pathTypeName, Type handlerType) {
				this.service = service;
				this.pathTypeName = pathTypeName;
				this.handlerType = handlerType;
				contexts = new Dictionary<string, IPathContext>();
			}

			public string PathTypeName {
				get { return pathTypeName; }
			}

			public IRequestHandler Handler {
				get {
					if (handler == null)
						handler = Activator.CreateInstance(handlerType, true) as IRequestHandler;
					return handler;
				}
			}

			public IPathContext GetContext(string pathName) {
				IPathContext context;
				if (!contexts.TryGetValue(pathName, out context)) {
					context = Handler.CreateContext(service.client, pathName);
					contexts[pathName] = context;
				}

				return context;
			}

			public PathTransaction CreateTransaction(string pathName) {
				IPathContext context = GetContext(pathName);
				IPathTransaction t = context.CreateTransaction();
				PathTransaction transaction = new PathTransaction(service, ++connId, context, t);
				transactions[transaction.Id] = transaction;
				return transaction;
			}

			public PathTransaction GetTransaction(int id) {
				PathTransaction transaction;
				if (transactions.TryGetValue(id, out transaction))
					return transaction;
				return null;
			}

			public void RemoveTransaction(PathTransaction transaction) {
				bool removed = transactions.Remove(transaction.Id);
				if (!removed)
					throw new InvalidOperationException();
			}
		}

		#endregion

		#region PathTransaction

		private sealed class PathTransaction : IPathTransaction {
			private readonly PathClientService service;
			private readonly IPathContext context;
			private readonly int id;
			private readonly IPathTransaction transaction;

			public PathTransaction(PathClientService service, int id, IPathContext context, IPathTransaction transaction) {
				this.service = service;
				this.id = id;
				this.transaction = transaction;
				this.context = context;
			}

			public int Id {
				get { return id; }
			}

			public void Dispose() {
				Transaction.Dispose();

				HandlerContainer container = service.GetMethodHandler(context.PathName);
				container.RemoveTransaction(this);

				service.OnTransactionDisposed(context.PathName, Transaction);
			}

			public IPathContext Context {
				get { return context; }
			}

			public IPathTransaction Transaction {
				get { return transaction; }
			}

			public DataAddress Commit() {
				DataAddress address = Transaction.Commit();

				HandlerContainer container = service.GetMethodHandler(context.PathName);
				container.RemoveTransaction(this);

				service.OnTransactionCommitted(context.PathName, Transaction, address);
				return address;
			}
		}

		#endregion
	}
}