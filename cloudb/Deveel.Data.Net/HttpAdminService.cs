using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net {
	public sealed class HttpAdminService : AdminService {
		private readonly string password;
		private readonly HttpServiceAddress address;
		private readonly NetworkConfigSource config;
		private HttpServiceConnector connector;
		private IMessageSerializer serializer;
		private bool polling;
				
		public HttpAdminService(IAdminServiceDelegator delegator, NetworkConfigSource config, Uri uri, string password)
			: base(new HttpServiceAddress(uri), new HttpServiceConnector(password), delegator) {
			this.config = config;
			this.password = password;
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
			Timer timer = new Timer(ConfigUpdate);

			try {
				// Schedule a refresh of the config file,
				// (We add a little entropy to ensure the network doesn't get hit by
				//  synchronized requests).
				Random r = new Random();
				long second_mix = r.Next(1000);
				timer.Change(50 * 1000, ((2 * 59) * 1000) + second_mix);

				HttpListener listener;
				try {
					listener = new HttpListener();
					listener.Prefixes.Add(address.ToString());
					listener.Start();
				} catch (IOException e) {
					//TODO: ERROR log ...
					return;
				}

				//TODO: INFO log ...

				while (polling) {
					HttpListenerContext context;
					try {
						// The socket to run the service,
						context = listener.GetContext();
						// Make sure this ip address is allowed,
						IPAddress ipAddress = ((IPEndPoint)context.Request.RemoteEndPoint).Address;

						//TODO: INFO log ...

						// Check it's allowed,
						if (ipAddress.IsIPv6LinkLocal ||
							IPAddress.IsLoopback(ipAddress) ||
							config.IsIpAllowed(ipAddress.ToString())) {
							// Dispatch the connection to the thread pool,
							HttpConnection c = new HttpConnection(this);
							ThreadPool.QueueUserWorkItem(c.Work, context);
						} else {
							//TODO: ERROR log ...
						}

					} catch (IOException e) {
						//TODO: WARN log ...
					}
				}
			} finally {
				timer.Dispose();
			}
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
					// Read the password string from the stream,
					string passwordCode = context.Request.Headers["Password"];

					// If it doesn't match, terminate the thread immediately,
					if (String.IsNullOrEmpty(passwordCode) ||
					    !passwordCode.Equals(service.password))
						return;

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