using System;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace Deveel.Data.Net.Security {
	[Serializable]
	public class OAuthResource : IDisposable, ISerializable {
		private bool alreadyDisposed;
		private HttpWebResponse response;
		private MemoryStream responseStream;

		public OAuthResource(HttpWebResponse response) {
			this.response = response;

			// Read and store response stream
			byte[] buffer;
			int totalRead;
			using (Stream stream = response.GetResponseStream()) {
				buffer = new byte[(ContentLength > 0) ? ContentLength : 4096];
				totalRead = 0;

				int thisRead = 0;
				while ((thisRead = stream.Read(buffer, totalRead, buffer.Length - totalRead)) != 0) {
					totalRead += thisRead;

					if (totalRead == buffer.Length) {
						// Increase by 4096 bytes at a time
						byte[] newBuffer = new byte[buffer.Length + 4096];
						buffer.CopyTo(newBuffer, 0);
						buffer = newBuffer;
					}
				}
			}

			responseStream = new MemoryStream(buffer, 0, totalRead, false, false);
			responseStream.Position = 0;
		}

		protected OAuthResource(SerializationInfo info, StreamingContext context) {
			response = (HttpWebResponse)info.GetValue("response", typeof(HttpWebResponse));
			responseStream = (MemoryStream)info.GetValue("responseStream", typeof(MemoryStream));
		}

		public string CharacterSet {
			get { return response.CharacterSet; }
		}

		public string ContentEncoding {
			get { return response.ContentEncoding; }
		}

		public long ContentLength {
			get { return response.ContentLength; }
		}

		public string ContentType {
			get { return response.ContentType; }
		}

		public CookieCollection Cookies {
			get { return response.Cookies; }
			set { response.Cookies = value; }
		}

		public WebHeaderCollection Headers {
			get { return response.Headers; }
		}

		public bool IsFromCache {
			get { return response.IsFromCache; }
		}

		public bool IsMutuallyAuthenticated {
			get { return response.IsMutuallyAuthenticated; }
		}

		public DateTime LastModified {
			get { return response.LastModified; }
		}

		public string Method {
			get { return response.Method; }
		}

		public Version ProtocolVersion {
			get { return response.ProtocolVersion; }
		}

		public Uri ResponseUri {
			get { return response.ResponseUri; }
		}

		public string Server {
			get { return response.Server; }
		}

		public HttpStatusCode StatusCode {
			get { return response.StatusCode; }
		}

		public string StatusDescription {
			get { return response.StatusDescription; }
		}

		public Stream GetResponseStream() {
			return responseStream;
		}

		public void Close() {
			response.Close();
		}

		public string GetResponseHeader(string headerName) {
			return response.GetResponseHeader(headerName);
		}

		[SecurityPermission(SecurityAction.LinkDemand, Flags = SecurityPermissionFlag.SerializationFormatter)]
		public virtual void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("response", response, typeof(HttpWebResponse));
			info.AddValue("responseStream", responseStream, typeof(MemoryStream));
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool isDisposing) {
			if (alreadyDisposed)
				return;

			if (isDisposing) {
				response = null;

				if (responseStream != null) {
					responseStream.Dispose();
					responseStream = null;
				}
			}

			this.alreadyDisposed = true;
		}
	}
}