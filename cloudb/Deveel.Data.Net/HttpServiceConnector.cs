using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Deveel.Data.Net {
	public sealed class HttpServiceConnector : IServiceConnector {		
		public HttpServiceConnector(string userName, string password) {
			this.userName = userName;
			this.password = password;
		}
		
		public HttpServiceConnector()
			: this(null, null) {
		}
		
		private string userName;
		private string password;
		private IMessageSerializer serializer;
		
		public IMessageSerializer Serializer {
			get { 
				if (serializer == null)
					serializer = new XmlMessageStreamSerializer();
				return serializer;
			}
			set { serializer = value; }
		}
	
		void IDisposable.Dispose() {
		}

		void IServiceConnector.Close() {
		}
		
		public IMessageProcessor Connect(HttpServiceAddress address, ServiceType serviceType) {
			return new HttpMessageProcessor(this, address, serviceType);
		}

		IMessageProcessor IServiceConnector.Connect(IServiceAddress address, ServiceType type) {
			return Connect((HttpServiceAddress)address, type);
		}
		
		#region HttpMessageProcessor
		
		class HttpMessageProcessor : IMessageProcessor {
			private readonly HttpServiceConnector connector;
			private readonly ServiceType serviceType;
			private readonly HttpServiceAddress address;
			
			public HttpMessageProcessor(HttpServiceConnector connector, HttpServiceAddress address, ServiceType serviceType) {
				this.connector = connector;
				this.address = address;
				this.serviceType = serviceType;
			}
			
			private MessageStream DoProcess(MessageStream messageStream, int tryCount) {
				try {
					HttpWebRequest request = (HttpWebRequest) HttpWebRequest.Create(address.ToUri());
					lock (request) {
						// Write the message.
						request.Headers["Service-Type"] = serviceType.ToString();
						if (!String.IsNullOrEmpty(connector.userName) &&
						    !String.IsNullOrEmpty(connector.password))
							request.Credentials = new NetworkCredential(connector.userName, connector.password);
						request.Method = "POST";
						Stream output = request.GetRequestStream();
						connector.Serializer.Serialize(messageStream, output);
						output.Flush();
						output.Close();
						
						HttpWebResponse response = (HttpWebResponse) request.GetResponse();
						if (response.StatusCode != HttpStatusCode.OK)
							throw new InvalidOperationException();
												
						Stream input = response.GetResponseStream();
						return connector.Serializer.Deserialize(input);
					}
				} catch (Exception e) {
					if (tryCount == 0 && e is WebException)
						// retry ...
						return DoProcess(messageStream, tryCount + 1);

					ServiceException error;
					if (e is WebException) {
						error = new ServiceException(new Exception("Web Error: maybe a timeout in the request.", e));
					} else {
						// Report this error as a msg_stream fault,
						error = new ServiceException(new Exception(e.Message, e));
					}

					MessageStream outputStream = new MessageStream(16);
					outputStream.AddMessage("E", error);
					return outputStream;
				}
			}

			
			public MessageStream Process(MessageStream messageStream) {
				return DoProcess(messageStream, 0);
			}
		}
		
		#endregion		
	}
}