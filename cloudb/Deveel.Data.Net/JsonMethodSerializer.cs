using System;
using System.IO;
using System.Text;

using Deveel.Json;

namespace Deveel.Data.Net {
	public sealed class JsonMethodSerializer : ITextMethodSerializer {
		private Encoding encoding;

		public Encoding ContentEncoding {
			get {
				if (encoding == null)
					encoding = Encoding.UTF8;
				return encoding;
			}
			set { encoding = value; }
		}

		private static object ConvertValue(object value, string typeName) {
			throw new NotImplementedException();
		}

		public void DeserializeRequest(MethodRequest request, Stream input) {
			DeserializeRequest(request, new StreamReader(input, ContentEncoding));
		}

		public void DeserializeRequest(MethodRequest request, TextReader reader) {
			JSONObject obj = new JSONObject(new JSONReader(reader));
			if (!obj.HasValue("request"))
				throw new FormatException();

			JSONObject jsonRequest = obj.GetValue<JSONObject>("request");
			foreach(string argName in jsonRequest.Keys) {
				object argValue = jsonRequest.GetValue<object>(argName);
				string format = null;

				if (argValue is JSONObject) {
					JSONObject jsonArgValue = (JSONObject) argValue;
					string typeName = jsonArgValue.GetValue<string>("type");
					format = jsonArgValue.GetValue<string>("format");

					argValue = ConvertValue(argValue, typeName);
				}

				MethodArgument arg = new MethodArgument(argName, argValue);
				if (!String.IsNullOrEmpty(format))
					arg.Format = format;

				request.Arguments.Add(arg);
			}
		}

		public void SerializeResponse(MethodResponse response, Stream output) {
			throw new NotImplementedException();
		}

		string ITextMethodSerializer.ContentEncoding {
			get { return ContentEncoding.BodyName; }
		}

		string ITextMethodSerializer.ContentType {
			get { return "application/json"; }
		}
	}
}