using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

using Deveel.Data.Diagnostics;
using Deveel.Data.Net.Client;

namespace Deveel.Data.Net {
	public sealed class HttpAdminService : AdminService {
		private IMessageSerializer serializer;
		private bool polling;
		private HttpListener listener;
		private Thread pollingThread;
				
		public HttpAdminService(IAdminServiceDelegator delegator, Uri uri)
			: this(delegator, new HttpServiceAddress(uri)) {
		}
		
		public HttpAdminService(IAdminServiceDelegator delegator, HttpServiceAddress address)
			: base(address, new HttpServiceConnector(), delegator) {
		}
		
		public IMessageSerializer Serializer {
			get { 
				if (serializer == null)
					serializer = new XmlMessageStreamSerializer();
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
						IPAddress ipAddress = ((IPEndPoint)context.Request.RemoteEndPoint).Address;

						Logger.Info("Connection opened with HTTP client " + ipAddress);

						// Check it's allowed,
						if (context.Request.IsLocal ||
							IsAddressAllowed(ipAddress.ToString())) {
							// Dispatch the connection to the thread pool,
							HttpConnection c = new HttpConnection(this);
							ThreadPool.QueueUserWorkItem(c.Work, context);
						} else {
							context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
							context.Response.Close();
							Logger.Error(String.Format("The client IP address {0} is not authorized", ipAddress));
						}

					} catch (IOException e) {
						Logger.Warning(e);
					}
				}
			} catch(Exception e) {
				Logger.Error(e.Message);
			}
		}

		protected override void OnInit() {
			base.OnInit();
			
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
		
		protected override void OnDispose(bool disposing) {
			if (disposing) {
				polling = false;
				
				if (listener != null) {
					listener.Stop();
					listener = null;
				}
			}
			
			base.OnDispose(disposing);
		}
		
		#region HttpConnection

		private class HttpConnection {
			private readonly HttpAdminService service;

			public HttpConnection(HttpAdminService service) {
				this.service = service;
			}

			private static MessageStream NoServiceError() {
				MessageStream msg_out = new MessageStream(16);
				msg_out.AddErrorMessage(new ServiceException(new Exception("The service requested is not being run on the instance")));
				return msg_out;
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
						// Read the command destination,
						string serviceTypeString =  context.Request.Headers["Service-Type"];
						if (String.IsNullOrEmpty(serviceTypeString) ||
						    String.Compare(serviceTypeString, "Error", true) == 0)
							return;
						
						ServiceType serviceType = (ServiceType)Enum.Parse(typeof(ServiceType), serviceTypeString, true);

						// Read the message stream object
						IMessageSerializer serializer = service.Serializer;
						MessageStream message_stream = serializer.Deserialize(context.Request.InputStream);

						MessageStream message_out;

						// For analytics
						DateTime benchmark_start = DateTime.Now;

						// Destined for the administration module,
						if (serviceType == ServiceType.Admin) {
							message_out = service.Processor.Process(message_stream);
						}
							// For a block service in this machine
						else if (serviceType == ServiceType.Block) {
							if (service.Block == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Block.Processor.Process(message_stream);
							}

						}
							// For a manager service in this machine
						else if (serviceType == ServiceType.Manager) {
							if (service.Manager == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Manager.Processor.Process(message_stream);
							}
						}
							// For a root service in this machine
						else if (serviceType == ServiceType.Root) {
							if (service.Root == null) {
								message_out = NoServiceError();
							} else {
								message_out = service.Root.Processor.Process(message_stream);
							}
						} else {
							throw new InvalidOperationException("Invalid destination service.");
						}

						// Update the stats
						DateTime benchmark_end = DateTime.Now;
						TimeSpan time_took = benchmark_end - benchmark_start;
						service.Analytics.AddEvent(benchmark_end, time_took);

						// Write and flush the output message,
						context.Response.StatusCode = (int)HttpStatusCode.OK;
						if (serializer is ITextMessageSerializer) {
							ITextMessageSerializer textSerializer = (ITextMessageSerializer) serializer;
							context.Response.ContentEncoding = Encoding.GetEncoding(textSerializer.ContentEncoding);
							context.Response.ContentType = textSerializer.ContentType;
						}
						serializer.Serialize(message_out, context.Response.OutputStream);
						context.Response.OutputStream.Flush();
						context.Response.Close();
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