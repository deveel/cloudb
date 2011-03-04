using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Net.Security {
	[Serializable]
	public class AuthenticationException : ApplicationException {
		private readonly int code;

		public AuthenticationException(string message, int code)
			: base(message) {
			this.code = code;
		}

		public AuthenticationException(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			code = info.GetInt32("Code");
		}

		public AuthenticationException(int code)
			: this(null, code) {
		}

		public virtual int Code {
			get { return code; }
		}

		public override void GetObjectData(SerializationInfo info, StreamingContext context) {
			base.GetObjectData(info, context);

			info.AddValue("Code", code);
		}
	}
}