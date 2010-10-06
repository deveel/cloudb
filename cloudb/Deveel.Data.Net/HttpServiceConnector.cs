using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class HttpServiceConnector : IServiceConnector {		
		public HttpServiceConnector(string userName, string password) {
			this.userName = userName;
			this.password = password;

			connected = true;
		}
		
		public HttpServiceConnector()
			: this(null, null) {
		}
		
		private string userName;
		private string password;
		private IMessageSerializer messageSerializer;
		private bool connected;

		public bool IsConnected {
			get { return connected; }
		}

		public IMessageSerializer MessageSerializer {
			get { 
				if (messageSerializer == null)
					messageSerializer = new XmlRpcMessageSerializer();
				return messageSerializer;
			}
			set { messageSerializer = value; }
		}
	
		void IDisposable.Dispose() {
			connected = false;
		}

		void IServiceConnector.Close() {
			connected = false;
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
			
			private MessageResponse DoProcess(MessageRequest messageStream, int tryCount) {
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
						connector.MessageSerializer.Serialize(messageStream, output);
						output.Flush();
						output.Close();
						
						HttpWebResponse response = (HttpWebResponse) request.GetResponse();
						if (response.StatusCode != HttpStatusCode.OK)
							throw new InvalidOperationException();
												
						Stream input = response.GetResponseStream();
						MessageResponse messageResponse = messageStream.CreateResponse();
						connector.MessageSerializer.Deserialize(messageResponse, input);
						return messageResponse;
					}
				} catch (Exception e) {
					if (tryCount == 0 && e is WebException)
						// retry ...
						return DoProcess(messageStream, tryCount + 1);

					MessageError error;
					if (e is WebException) {
						error = new MessageError(new Exception("Web Error: maybe a timeout in the request.", e));
					} else {
						// Report this error as a msg_stream fault,
						error = new MessageError(new Exception(e.Message, e));
					}

					MessageResponse response = messageStream.CreateResponse("E");
					response.Code = MessageResponseCode.Error;
					response.Arguments.Add(error);
					return response;
				}
			}

			
			public Message ProcessMessage(Message message) {
				if (!(message is MessageRequest))
					throw new ArgumentException();

				return DoProcess(((MessageRequest) message), 0);
			}
		}
		
		#endregion		
	}
}