using System;

namespace Deveel.Data.Net {
	public sealed class MethodRequest : ICloneable {
		private readonly MethodType type;
		private readonly IPathTransaction transaction;
		private ArgumentList arguments;
		private MethodResponse response;

		public const string ResourceIdName = "resource-id";

		internal MethodRequest(MethodType type, IPathTransaction transaction) {
			this.type = type;
			this.transaction = transaction;
			arguments = new ArgumentList(false);
		}

		public MethodType Type {
			get { return type; }
		}

		public IPathTransaction Transaction {
			get { return transaction; }
		}

		public ArgumentList Arguments {
			get { return arguments; }
		}

		public bool HasResourceId {
			get { return arguments.Contains(ResourceIdName); }
		}

		public object ResourceId {
			get { 
				MethodArgument argument = arguments[ResourceIdName];
				return argument != null ? argument.Value : null;
			}
		}

		public object Clone() {
			MethodRequest request = new MethodRequest(type, transaction);
			request.arguments = (ArgumentList) arguments.Clone();
			return request;
		}

		public MethodResponse CreateResponse() {
			if (response != null)
				throw new InvalidOperationException("A response was previously created.");

			response = new MethodResponse(this, transaction);
			return response;
		}
	}
}