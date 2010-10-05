using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;

namespace Deveel.Data.Net {
	public sealed class RestPathService : PathService {
		private HttpListener listener;
		private Thread pollingThread;
		private bool polling;
		
		public RestPathService(HttpServiceAddress address, IServiceAddress managerAddress, IServiceConnector connector)
			: base(address, managerAddress, connector) {
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
				//TODO: for the moment we don't use it ...
				// listener.AuthenticationSchemes = AuthenticationSchemes.Basic;
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
		
		#region HttpConnection

		private class HttpConnection {
			private readonly RestPathService service;

			public HttpConnection(RestPathService service) {
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
						string method = context.Request.HttpMethod;
						MethodType methodType = (MethodType)Enum.Parse(typeof(MethodType), method, true);
						
						string pathName = context.Request.Url.PathAndQuery;
						string resourceId = null;
						int tid = -1;
						
						if (String.IsNullOrEmpty(pathName))
							throw new InvalidOperationException("None path specified.");
						
						if (pathName[0] == '/')
							pathName = pathName.Substring(1);
						if (pathName[pathName.Length - 1] == '/')
							pathName = pathName.Substring(0, pathName.Length - 1);

						int index = pathName.IndexOf('?');
						if (index != -1) {
							//TODO: extract the transaction id from the query ...
							pathName = pathName.Substring(index + 1);
						}

						index = pathName.IndexOf('/');
						if (index != -1) {
							resourceId = pathName.Substring(index + 1);
							pathName = pathName.Substring(0, index);
						}
						
						Dictionary<string, object> args = null;
						index = resourceId.IndexOf('/');
						if (index != -1) {
							string id = resourceId.Substring(index + 1);
							resourceId = resourceId.Substring(0, index);
							
							args = new Dictionary<string, object>();
							args["id"] = id;
						}
						
						Stream requestStream = null;
						if (methodType == MethodType.Post ||
							methodType == MethodType.Put)
							requestStream = context.Request.InputStream;

						MethodResponse response = service.HandleRequest(methodType, pathName, resourceId, tid, args, requestStream);

						if (requestStream != null)
							requestStream.Close();
						
						if (response.Code == MethodResponseCode.NotFound) {
							context.Response.StatusCode = 404;
						} else if (response.Code == MethodResponseCode.UnsupportedFormat) {
							context.Response.StatusCode = 415;
						} else if (response.Code == MethodResponseCode.Error) {
							context.Response.StatusCode = 500;
							//TODO: write down the error...
						} else if (response.Code == MethodResponseCode.Success) {
							if (methodType == MethodType.Post ||
							    methodType == MethodType.Put)
								context.Response.StatusCode = 201;
							else if (methodType == MethodType.Delete)
								context.Response.StatusCode = 204;
							else
								context.Response.StatusCode = 200;
							
							// Write and flush the output message,
							Stream responseStream = context.Response.OutputStream;
							service.MethodSerializer.SerializeResponse(response, responseStream);
							responseStream.Flush();
							responseStream.Close();
						}
						
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