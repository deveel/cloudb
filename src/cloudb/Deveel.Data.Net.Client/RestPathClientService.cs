using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace Deveel.Data.Net.Client {
	public sealed class RestPathClientService : PathClientService {
		private HttpListener listener;
		private Thread pollingThread;
		private bool polling;
		private RestFormat format;
		
		public RestPathClientService(HttpServiceAddress address, IServiceAddress managerAddress, IServiceConnector connector)
			: base(address, managerAddress, connector) {
		}

		protected override string Type {
			get { return "rest"; }
		}

		public override IMessageSerializer MessageSerializer {
			get { return base.MessageSerializer; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");
				if (!(value is IRestMessageSerializer))
					throw new ArgumentException("The given serializer is not REST compatible.");

				format = ((IRestMessageSerializer) value).Format;
				base.MessageSerializer = value;
			}
		}

		public RestFormat Format {
			get { return format; }
		}

		private void Poll() {
			try {
				polling = true;

				while (polling) {
					HttpListenerContext context;
					try {
						// The socket to run the service,
						context = listener.GetContext();
						// Make sure this ip address is allowed,
						IPAddress ipAddress = null;
						if (context.Request.RemoteEndPoint != null)
							ipAddress = context.Request.RemoteEndPoint.Address;

						Logger.Info("Connection opened with HTTP client " + ipAddress);

						// Dispatch the connection to the thread pool,
						HttpConnection c = new HttpConnection(this);
						ThreadPool.QueueUserWorkItem(c.Work, context);
					} catch(IOException e) {
						Logger.Warning(e);
					}
				}
			} catch(Exception e) {
				Logger.Error(e.Message);
			}
		}

		protected override void OnInit() {
			try {
				listener = new HttpListener();
				listener.Prefixes.Add(Address.ToString());
				listener.Start();
			} catch (Exception e) {
				Logger.Error("Error Starting the HTTP Listener", e);
				throw new ApplicationException("Cannot start HTTP listener: " + e.Message, e);
			}
			
			Logger.Info(String.Format("Node started listening HTTP connections on {0}", Address));

			pollingThread = new Thread(Poll);
			pollingThread.Name = "TCP Path Service Polling";
			pollingThread.IsBackground = true;
			pollingThread.Start();

			base.OnInit();
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				polling = false;
				listener.Stop();
				listener = null;
			}

			base.Dispose(disposing);
		}
		
		#region HttpConnection

		private class HttpConnection {
			private readonly RestPathClientService service;

			public HttpConnection(RestPathClientService service) {
				this.service = service;
			}

			public void Work(object state) {
				HttpListenerContext context = (HttpListenerContext)state;

				try {
					// Get the credentials if specified,
					if (context.User != null) {
						HttpListenerBasicIdentity identity = (HttpListenerBasicIdentity)context.User.Identity;
						IPathClientAuthorize authorize = service.Authorize;
						if (authorize != null && !authorize.IsAuthorized(identity)) {
							context.Response.StatusCode = 401;
							context.Response.Close();
							return;
						}
					}

					// The main command dispatch loop for this connection,

					string method = context.Request.HttpMethod;
					RequestType requestType = (RequestType)Enum.Parse(typeof(RequestType), method, true);

					string pathName = context.Request.Url.PathAndQuery;
					string resourceId = null;

					if (String.IsNullOrEmpty(pathName))
						throw new InvalidOperationException("None path specified.");

					if (pathName[0] == '/')
						pathName = pathName.Substring(1);
					if (pathName[pathName.Length - 1] == '/')
						pathName = pathName.Substring(0, pathName.Length - 1);

					int index = pathName.IndexOf('?');

					Dictionary<string, object> args = new Dictionary<string, object>();
					if (index != -1) {
						//TODO: extract the transaction id from the query ...
						string query = pathName.Substring(index + 1);
						pathName = pathName.Substring(0, index);

						if (!String.IsNullOrEmpty(query)) {
							string[] sp = query.Split('&');
							for (int i = 0; i < sp.Length; i++) {
								string s = sp[i].Trim();
								int idx = s.IndexOf('=');
								if (idx == -1) {
									args[s] = String.Empty;
								} else {
									string key = s.Substring(0, index);
									string value = s.Substring(index + 1);
									args[key] = value;
								}
							}
						}
					}

					index = pathName.IndexOf('/');
					if (index != -1) {
						resourceId = pathName.Substring(index + 1);
						pathName = pathName.Substring(0, index);
					}

					if (!String.IsNullOrEmpty(resourceId)) {
						index = resourceId.IndexOf('/');
						if (index != -1) {
							string id = resourceId.Substring(index + 1);
							resourceId = resourceId.Substring(0, index);
							args[RequestMessage.ItemIdName] = id;
						}

						args[RequestMessage.ResourceIdName] = resourceId;
					}

					Stream requestStream = null;
					if (requestType == RequestType.Post ||
						requestType == RequestType.Put)
						requestStream = context.Request.InputStream;

					ResponseMessage response = service.HandleRequest(requestType, pathName, args, requestStream);

					if (requestStream != null)
						requestStream.Close();

					if (response.Code == MessageResponseCode.NotFound) {
						context.Response.StatusCode = 404;
					} else if (response.Code == MessageResponseCode.UnsupportedFormat) {
						context.Response.StatusCode = 415;
					} else if (response.Code == MessageResponseCode.Error) {
						context.Response.StatusCode = 500;
						if (response.Arguments.Contains("message")) {
							MessageArgument messageArg = response.Arguments["message"];
							byte[] bytes = context.Response.ContentEncoding.GetBytes(messageArg.ToString());
							context.Response.OutputStream.Write(bytes, 0, bytes.Length);
							context.Response.OutputStream.Flush();
						}
					} else if (response.Code == MessageResponseCode.Success) {
						if (requestType == RequestType.Post ||
							requestType == RequestType.Put)
							context.Response.StatusCode = 201;
						else if (requestType == RequestType.Delete) {
							context.Response.StatusCode = 204;
							context.Response.Close();
							return;
						} else
							context.Response.StatusCode = 200;

						// TODO: make it recurive ...
						if (!response.Request.HasItemId) {
							foreach(MessageArgument argument in response.Arguments) {
								if (argument.HasId) {
									StringBuilder href = new StringBuilder(service.Address.ToString());
									if (href[href.Length - 1] != '/')
										href.Append("/");
									href.Append(response.Request.ResourceId);
									href.Append("/");
									href.Append(argument.Id);
									href.Append("/");
									argument.Attributes["href"] = href.ToString();
								}
							}
						}

						// Write and flush the output message,
						Stream responseStream = context.Response.OutputStream;
						service.MessageSerializer.Serialize(response, responseStream);
						responseStream.Flush();
						responseStream.Close();
					}
				} catch (IOException e) {
					context.Response.StatusCode = 500;

					if (e is EndOfStreamException) {
						// Ignore this one also,
					} else {
						//TODO: ERROR log ...
					}
				} finally {
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