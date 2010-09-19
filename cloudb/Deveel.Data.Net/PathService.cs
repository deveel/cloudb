using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;

namespace Deveel.Data.Net {
	public abstract class PathService : Component {
		protected PathService(IServiceAddress address, IServiceAddress managerAddress, IServiceConnector connector) {
			this.address = address;
			this.managerAddress = managerAddress;
			this.connector = connector;

			network = new NetworkProfile(connector);
		}

		~PathService() {
			Dispose(false);
		}

		private readonly IServiceAddress address;
		private readonly IServiceAddress managerAddress;
		private readonly IServiceConnector connector;
		private IMethodSerializer methodSerializer;

		private NetworkClient client;
		private readonly NetworkProfile network;

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

		public IMethodSerializer MethodSerializer {
			get {
				if (methodSerializer == null)
					methodSerializer = new BinaryMethodSerializer();
				return methodSerializer;
			}
			set { methodSerializer = value; }
		}

		protected bool IsConnected {
			get { return client != null && client.IsConnected; }
		}

		private void ScanForHandlers() {
			if (handlers.Count == 0)
				return;

			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Type[] types = assemblies[i].GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (type != typeof(IMethodHandler) &&
						typeof(IMethodHandler).IsAssignableFrom(type) &&
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

		private MethodRequest GetMethodRequest(MethodType type, IPathTransaction transaction, Stream requestStream) {
			MethodRequest request = new MethodRequest(type, transaction);
			MethodSerializer.DeserializeRequest(request, requestStream);
			request.Arguments.Seal();
			return request;
		}

		protected MethodResponse HandleRequest(MethodType type, IPathTransaction transaction, Stream requestStream) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			string pathName = transaction.Context.PathName;
			HandlerContainer handler = GetMethodHandler(pathName);
			if (handler == null)
				throw new InvalidOperationException();

			MethodRequest request = GetMethodRequest(type, transaction, requestStream);
			return handler.Handler.HandleRequest(request);
		}

		protected MethodResponse HandleRequest(MethodType type, string pathName, int tid, Stream requestStream) {
			HandlerContainer handler = GetMethodHandler(pathName);
			if (handler == null)
				throw new InvalidOperationException();

			if (tid != -1) {
				PathTransaction transaction = handler.GetTransaction(tid);
				if (transaction == null)
					throw new ArgumentException();

				if (transaction.Context.PathName != pathName)
					throw new InvalidOperationException();

				return HandleRequest(type, transaction, requestStream);
			}

			return HandleRequest(type, pathName, requestStream);
		}

		protected MethodResponse HandleRequest(MethodType type, string pathName, Stream requestStream) {
			HandlerContainer handler = GetMethodHandler(pathName);
			if (handler == null)
				throw new InvalidOperationException();

			PathTransaction transaction = handler.CreateTransaction(pathName);
			return HandleRequest(type, transaction, requestStream);
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
			private readonly PathService service;
			private readonly string pathTypeName;
			private readonly Type handlerType;
			private IMethodHandler handler;
			private readonly Dictionary<string, IPathContext> contexts;

			private int connId = -1;
			private readonly Dictionary<int, PathTransaction> transactions = new Dictionary<int, PathTransaction>();

			public HandlerContainer(PathService service, string pathTypeName, Type handlerType) {
				this.service = service;
				this.pathTypeName = pathTypeName;
				this.handlerType = handlerType;
				contexts = new Dictionary<string, IPathContext>();
			}

			public string PathTypeName {
				get { return pathTypeName; }
			}

			public IMethodHandler Handler {
				get {
					if (handler == null)
						handler = Activator.CreateInstance(handlerType, true) as IMethodHandler;
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
			private readonly PathService service;
			private readonly IPathContext context;
			private readonly int id;
			private readonly IPathTransaction transaction;

			public PathTransaction(PathService service, int id, IPathContext context, IPathTransaction transaction) {
				this.service = service;
				this.id = id;
				this.transaction = transaction;
				this.context = context;
			}

			public int Id {
				get { return id; }
			}
			public void Dispose() {
				transaction.Dispose();

				HandlerContainer container = service.GetMethodHandler(context.PathName);
				container.RemoveTransaction(this);

				service.OnTransactionDisposed(context.PathName, transaction);
			}

			public IPathContext Context {
				get { return context; }
			}

			public DataAddress Commit() {
				DataAddress address = transaction.Commit();

				HandlerContainer container = service.GetMethodHandler(context.PathName);
				container.RemoveTransaction(this);

				service.OnTransactionCommitted(context.PathName, transaction, address);
				return address;
			}
		}

		#endregion
	}
}