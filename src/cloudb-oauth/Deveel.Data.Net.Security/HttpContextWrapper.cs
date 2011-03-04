using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Security.Principal;
using System.Text;
using System.Web;

namespace Deveel.Data.Net.Security {
	static class HttpContextWrapper {
		public static IHttpContext Wrap(object context) {
			if (context is HttpContext)
				return new HttpContextImpl((HttpContext) context);
			if (context is HttpListenerContext)
				return new HttpListenerContextImpl((HttpListenerContext) context);
			if (context is IHttpContext)
				return (IHttpContext) context;

			throw new ArgumentException("Invalid HTTP context.");
		}

		#region HttpContextImpl

		private class HttpContextImpl : IHttpContext {
			private readonly HttpContext context;
			private readonly HttpRequestImpl request;
			private readonly HttpResponseImpl response;

			public HttpContextImpl(HttpContext context) {
				this.context = context;
				request = new HttpRequestImpl(context.Request);
				response = new HttpResponseImpl(context.Response);
			}

			public IHttpRequest Request {
				get { return request; }
			}

			public IHttpResponse Response {
				get { return response; }
			}

			public IPrincipal User {
				get { return context.User; }
				set { context.User = value; }
			}

			#region HttpRequestImpl

			private class HttpRequestImpl : IHttpRequest {
				private readonly HttpRequest request;

				public HttpRequestImpl(HttpRequest request) {
					this.request = request;
				}

				public string HttpMethod {
					get { return request.HttpMethod; }
				}

				public Uri Url {
					get { return request.Url; }
				}

				public string RawUrl {
					get { return request.RawUrl; }
				}

				public NameValueCollection QueryString {
					get { return request.QueryString; }
				}

				public NameValueCollection Form {
					get { return request.Form; }
				}

				public NameValueCollection Headers {
					get { return request.Headers; }
				}
			}

			#endregion

			#region HttpResponseImpl

			private class HttpResponseImpl : IHttpResponse {
				private readonly HttpResponse response;

				public HttpResponseImpl(HttpResponse response) {
					this.response = response;
				}

				public Encoding ContentEncoding {
					get { return response.ContentEncoding; }
					set { response.ContentEncoding = value; }
				}

				public string ContentType {
					get { return response.ContentType; }
					set { response.ContentType = value; }
				}

				public int StatusCode {
					get { return response.StatusCode; }
					set { response.StatusCode = value; }
				}

				public Stream OutputStream {
					get { return response.OutputStream; }
				}

				public NameValueCollection Headers {
					get { return response.Headers; }
				}

				public void Close() {
					response.Close();
				}
			}

			#endregion
		}

		#endregion

		#region HttpListenerContextImpl

		private class HttpListenerContextImpl : IHttpContext {
			private readonly HttpListenerContext context;
			private readonly HttpListenerRequestImpl request;
			private readonly HttpListenerResponseImpl response;

			public HttpListenerContextImpl(HttpListenerContext context) {
				this.context = context;
				request = new HttpListenerRequestImpl(context.Request);
				response = new HttpListenerResponseImpl(context.Response);
			}

			public IHttpRequest Request {
				get { return request; }
			}

			public IHttpResponse Response {
				get { return response; }
			}

			public IPrincipal User {
				get { return context.User; }
				set { }
			}

			#region HttpListenerRequestImpl

			private class HttpListenerRequestImpl : IHttpRequest {
				private readonly HttpListenerRequest request;
				private NameValueCollection form = new NameValueCollection();
				private bool formParsed;

				public HttpListenerRequestImpl(HttpListenerRequest request) {
					this.request = request;
				}

				public string HttpMethod {
					get { return request.HttpMethod; }
				}

				public Uri Url {
					get { return request.Url; }
				}

				public string RawUrl {
					get { return request.RawUrl; }
				}

				public NameValueCollection QueryString {
					get { return request.QueryString; }
				}

				public NameValueCollection Form {
					get {
						if (request.HttpMethod == "POST" &&
							request.HasEntityBody) {
							if (!formParsed) {
								form = GetForm();
								formParsed = true;
							}
						}

						return form;
					}
				}

				public NameValueCollection Headers {
					get { return request.Headers; }
				}

				private NameValueCollection GetForm() {
					NameValueCollection collection = new NameValueCollection();

					string s;
					using (StreamReader reader = new StreamReader(request.InputStream)) {
						s = reader.ReadToEnd();
					}

					string[] sp = s.Split('&');

					for (int i = 0; i < sp.Length; i++) {
						string[] sp2 = sp[i].Split('=');
						string value = HttpUtility.HtmlDecode(sp2[1]);
						collection.Add(sp2[0], value ?? String.Empty);
					}

					return collection;
				}
			}

			#endregion

			#region HttpListenerResponseImpl

			private class HttpListenerResponseImpl : IHttpResponse {
				private readonly HttpListenerResponse response;

				public HttpListenerResponseImpl(HttpListenerResponse response) {
					this.response = response;
				}

				public Encoding ContentEncoding {
					get { return response.ContentEncoding; }
					set { response.ContentEncoding = value; }
				}

				public string ContentType {
					get { return response.ContentType; }
					set { response.ContentType = value; }
				}

				public int StatusCode {
					get { return response.StatusCode; }
					set { response.StatusCode = value; }
				}

				public Stream OutputStream {
					get { return response.OutputStream; }
				}

				public NameValueCollection Headers {
					get { return response.Headers; }
				}

				public void Close() {
					response.Close();
				}
			}

			#endregion
		}

		#endregion
	}
}