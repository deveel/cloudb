using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

using Deveel.Data.Net.Client;
using Deveel.Data.Net.Serialization;

namespace Deveel.Data.Net {
	public sealed class HttpAdminService : AdminService {
		private IMessageSerializer serializer;
		private bool polling;
		private HttpListener listener;
		private Thread pollingThread;
				
		public HttpAdminService(IServiceFactory serviceFactory, Uri uri)
			: this(serviceFactory, new HttpServiceAddress(uri)) {
		}
		
		public HttpAdminService(IServiceFactory serviceFactory, HttpServiceAddress address)
			: base(address, new HttpServiceConnector(), serviceFactory) {
		}
		
		public IMessageSerializer Serializer {
			get { 
				if (serializer == null)
					serializer = new XmlRpcMessageSerializer();
				return serializer;
			}
			set { serializer = value; }
		}
		
		private void Poll() {
			try {
				while (polling) {
					HttpListenerContext context;
					try {
						// The socket to run the service,
						context = listener.GetContext();						
						// Make sure this ip address is allowed,
						IPAddress ipAddress = context.Request.RemoteEndPoint.Address;

						Logger.Info("Connection opened with HTTP client " + ipAddress);

						string ipAddressString = ipAddress.ToString();
						bool authorized = context.Request.IsLocal || IsAddressAllowed(ipAddressString);

						authorized = OnClientConnect(ipAddressString, authorized);

						// Check it's allowed,)
						if (authorized) {
							// Dispatch the connection to the thread pool,
							HttpConnection c = new HttpConnection(this);
							ThreadPool.QueueUserWorkItem(c.Work, context);
						} else {
							context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
							context.Response.Close();
							Logger.Error(String.Format("The client IP address {0} is not authorized", ipAddressString));
						}
					} catch (IOException e) {
						Logger.Warning(e);
					}
				}
			} catch(Exception e) {
				Logger.Error(e.Message);
			}
		}

		protected override void OnStart() {
			base.OnStart();
			
			Logger.Info("Starting node");
			
			try {
				listener = new HttpListener();
				listener.Prefixes.Add(Address.ToString());
				//TODO: for the moment we don't use it ...
				// listener.AuthenticationSchemes = AuthenticationSchemes.Basic;
				listener.Start();
			} catch (Exception e) {
				Logger.Error("Error Starting the HTTP Listener", e);
				throw new ApplicationException("Cannot start HTTP listener: " + e.Message, e);
			}
				
			Logger.Info(String.Format("Node started listening HTTP connections on {0}", Address));

			polling = true;
			
			pollingThread = new Thread(Poll);
			pollingThread.IsBackground = true;
			pollingThread.Start();
		}
		
		protected override void OnStop() {
			polling = false;

			if (listener != null) {
				listener.Stop();
				listener = null;
			}

			base.OnStop();
		}
		
		#region HttpConnection

		private class HttpConnection {
			private readonly HttpAdminService service;

			public HttpConnection(HttpAdminService service) {
				this.service = service;
			}

			private static Message NoServiceError(RequestMessage request) {
				ResponseMessage response = request.CreateResponse();
				response.Arguments.Add(new MessageError(new Exception("The service requested is not being run on the instance")));
				return response;
			}

			public void Work(object state) {
				HttpListenerContext context = (HttpListenerContext)state;
				try {
					// Get the credentials if specified,
					if (context.User != null) {
						HttpListenerBasicIdentity identity = (HttpListenerBasicIdentity) context.User.Identity;
						if (identity != null) {
							string userName = identity.Name;
							string password = identity.Password;
								
							//TODO: verify if they're allowed ...
						}
					}

					// The main command dispatch loop for this connection,
					while (true) {
						IPAddress ipAddress = context.Request.RemoteEndPoint.Address;

						// Read the command destination,
						string serviceTypeString =  context.Request.Headers["Service-Type"];
						if (String.IsNullOrEmpty(serviceTypeString) ||
						    String.Compare(serviceTypeString, "Error", true) == 0)
							return;
						
						ServiceType serviceType = (ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeString, true);

						// Read the message stream object
						IMessageSerializer serializer = service.Serializer;
						RequestMessage requestMessage = (RequestMessage) serializer.Deserialize(context.Request.InputStream, MessageType.Request);

						Message responseMessage;

						service.OnClientRequest(serviceType, ipAddress.ToString(), requestMessage);

						// For analytics
						DateTime benchmarkStart = DateTime.Now;

						// Destined for the administration module,
						if (serviceType == ServiceType.Admin) {
							responseMessage = service.Processor.Process(requestMessage);
						}
							// For a block service in this machine
						else if (serviceType == ServiceType.Block) {
							if (service.Block == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Block.Processor.Process(requestMessage);
							}

						}
							// For a manager service in this machine
						else if (serviceType == ServiceType.Manager) {
							if (service.Manager == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Manager.Processor.Process(requestMessage);
							}
						}
							// For a root service in this machine
						else if (serviceType == ServiceType.Root) {
							if (service.Root == null) {
								responseMessage = NoServiceError(requestMessage);
							} else {
								responseMessage = service.Root.Processor.Process(requestMessage);
							}
						} else {
							throw new InvalidOperationException("Invalid destination service.");
						}

						service.OnClientResponse(ipAddress.ToString(), responseMessage);

						// Update the stats
						DateTime benchmarkEnd = DateTime.Now;
						TimeSpan timeTook = benchmarkEnd - benchmarkStart;
						service.Analytics.AddEvent(benchmarkEnd, timeTook);

						// Write and flush the output message,
						context.Response.StatusCode = (int)HttpStatusCode.OK;
						if (serializer is ITextMessageSerializer) {
							ITextMessageSerializer textSerializer = (ITextMessageSerializer) serializer;
							context.Response.ContentEncoding = Encoding.GetEncoding(textSerializer.ContentEncoding);
							context.Response.ContentType = textSerializer.ContentType;
						}
						serializer.Serialize(responseMessage, context.Response.OutputStream);
						context.Response.OutputStream.Flush();
						context.Response.Close();

						service.OnClientDisconnect(ipAddress.ToString());
					} // while (true)

				} catch (IOException e) {
					if (e is EndOfStreamException) {
						// Ignore this one also,
					} else {
						//TODO: ERROR log ...
					}
				}  finally {
					// Make sure the socket is closed before we return from the thread,
					try {
						context.Response.Close();
					} catch (IOException e) {
						//TODO: ERROR log ...
					}
				}
			}
		}

		#endregion

	}
}