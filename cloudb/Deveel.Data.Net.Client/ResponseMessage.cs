﻿using System;

namespace Deveel.Data.Net.Client {
	public class ResponseMessage : Message {
		private readonly RequestMessage request;
		private MessageResponseCode code;

		public ResponseMessage(string name, RequestMessage request)
			: base(name) {
			if (request != null)
				request.OnResponseMessageCreated(this);

			this.request = request;
		}

		public ResponseMessage(string name)
			: this(name, null) {
		}

		public ResponseMessage()
			: this(null) {
		}

		protected override MessageType MessageType {
			get { return MessageType.Response; }
		}

		public RequestMessage Request {
			get { return request; }
		}
		
		public MessageResponseCode Code {
			get {
				if (HasError && code == MessageResponseCode.Success)
					code = MessageResponseCode.Error;

				return code;
			}
			set { code = value; }
		}

		public bool HasError {
			get { return MessageUtil.HasError(this); }
		}

		public string ErrorMessage {
			get { return MessageUtil.GetErrorMessage(this); }
		}

		public string ErrorStackTrace {
			get { return MessageUtil.GetErrorStackTrace(this); }
		}

		public MessageError Error {
			get { return MessageUtil.GetError(this); }
		}

		public bool HasReturnValue {
			get { return Arguments.Count == 1 && !HasError; }
		}

		public object ReturnValue {
			get { return Arguments.Count == 1 && !HasError ? Arguments[0].Value : null; }
		}
	}
}