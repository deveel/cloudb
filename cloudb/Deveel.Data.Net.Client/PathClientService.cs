using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using System.Security.Principal;

using Deveel.Data.Diagnostics;

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

			log = LogManager.NetworkLogger;
		}

		~PathClientService() {
			Dispose(false);
		}

		private readonly IServiceAddress address;
		private readonly IServiceAddress managerAddress;
		private readonly IServiceConnector connector;
		private IMessageSerializer messageSerializer;
		private readonly Logger log;

		private IPathClientAuthorize authorize;

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

		public virtual IPathClientAuthorize Authorize {
			get { return authorize; }
			set { authorize = value; }
		}

		public virtual PathClienAuthorizeDelegate AuthorizeDelegate {
			get { return (authorize != null && authorize is DelegatedAuthorize) ? ((DelegatedAuthorize) authorize).Delegate : null; }
			set {
				if (value == null && (authorize != null && authorize is DelegatedAuthorize)) {
					authorize = null;
				} else {
					authorize = new DelegatedAuthorize(value);
				}
			}
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
					if (type != typeof(IMessageRequestHandler) &&
					    typeof(IMessageRequestHandler).IsAssignableFrom(type) &&
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

		private MessageRequest GetMethodRequest(RequestType type, PathTransaction transaction, Stream requestStream) {
			MessageRequest request = new ClientMessageRequest(Type, type, transaction.Transaction);
			if (requestStream != null)
				MessageSerializer.Deserialize(request, requestStream);
			return request;
		}

		protected MessageResponse HandleRequest(RequestType type, string pathName, IDictionary<string, object> args, Stream requestStream) {
			//TODO: allow having multiple handlers for the service ...
			HandlerContainer handler = GetMethodHandler(pathName);
			if (handler == null)
				throw new InvalidOperationException("No handler was found for the path '" + pathName + "' in this context.");

			if (!handler.Handler.CanHandleClientType(Type))
				throw new InvalidOperationException("The handler for the path '" + pathName + "' cannot support client of type '" + Type + "'.");

			IPathTransaction transaction;

			if (TransactionIdKey != null && (args != null && args.ContainsKey(TransactionIdKey))) {
				int tid = Convert.ToInt32(args[TransactionIdKey]);
				transaction = GetTransaction(pathName, tid);
			} else {
				transaction = CreateTransaction(pathName);
			}

			MessageRequest request = GetMethodRequest(type, ((PathTransaction) transaction), requestStream);
			if (args != null) {
				foreach(KeyValuePair<string, object> pair in args) {
					request.Attributes.Add(pair.Key, pair.Value);
				}
			}
			request.Seal();
			return (MessageResponse) handler.Handler.ProcessMessage(request);
		}
		
		protected MessageResponse HandleRequest(RequestType type, string pathName, Stream requestStream) {
			return HandleRequest(type, pathName, null, requestStream);
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
			private IMessageRequestHandler handler;
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

			public IMessageRequestHandler Handler {
				get {
					if (handler == null)
						handler = Activator.CreateInstance(handlerType, true) as IMessageRequestHandler;
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

		#region DelegatedAuthorize

		private class DelegatedAuthorize : IPathClientAuthorize {
			public readonly PathClienAuthorizeDelegate Delegate;

			public DelegatedAuthorize(PathClienAuthorizeDelegate wrapped) {
				Delegate = wrapped;
			}

			public bool IsAuthorized(IIdentity identity) {
				return Delegate(identity);
			}
		}

		#endregion
	}
}