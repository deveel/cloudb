using System;

namespace Deveel.Data.Net.Client {
	internal static class MessageUtil {
		public static bool HasError(Message message) {
			return GetError(message) != null;
		}

		public static string GetErrorMessage(Message message) {
			MessageError error = GetError(message);
			return error == null ? null : error.Message;
		}

		public static MessageError GetError(Message message) {
			if (message is ResponseMessageStream) {
				foreach (Message msg in (ResponseMessageStream)message) {
					MessageError error = GetError(msg);
					if (error != null)
						return error;
				}

				return null;
			}
			return message.Arguments.Count == 1 && message.Arguments[0].Value is MessageError
			       	? (MessageError) message.Arguments[0].Value
			       	: null;
		}

		public static string GetErrorStackTrace(Message message) {
			MessageError error = GetError(message);
			return error == null ? null : error.StackTrace;
		}
	}
}