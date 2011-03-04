using System;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Deveel.Data.Net.Security {
	public sealed class OAuthRequestContext {
		private readonly List<OAuthRequestException> errors = new List<OAuthRequestException>();
		private IRequestToken requestToken;
		private IAccessToken accessToken;
		private OAuthPrincipal principal;
		private OAuthParameters parameters;
		private ISignProvider signProvider;
		private IConsumer consumer;
		private RequestId requestId;
		private string signature;
		private bool signatureValid;
		private bool oauthRequest;
		private readonly NameValueCollection responseParameters;

		internal OAuthRequestContext() {
			responseParameters = new NameValueCollection();
		}

		public NameValueCollection ResponseParameters {
			get { return responseParameters; }
		}

		public OAuthParameters Parameters {
			get { return parameters; }
			set { parameters = value; }
		}

		public ISignProvider SignProvider {
			get { return signProvider; }
			set { signProvider = value; }
		}

		public IConsumer Consumer {
			get { return consumer; }
			set { consumer = value; }
		}

		public IRequestToken RequestToken {
			get {
				if (requestToken == null && accessToken != null)
					requestToken = accessToken.RequestToken;

				return requestToken;
			}
			set { requestToken = value; }
		}

		public IAccessToken AccessToken {
			get { return accessToken; }
			set { accessToken = value; }
		}

		public RequestId RequestId {
			get { return requestId; }
			set { requestId = value; }
		}

		public bool IsSignatureValid {
			get { return signatureValid; }
			set { signatureValid = value; }
		}

		public string Signature {
			get { return signature; }
			set { signature = value; }
		}

		public bool IsOAuthRequest {
			get { return oauthRequest; }
			set { oauthRequest = value; }
		}

		public OAuthPrincipal Principal {
			get { return principal; }
			set { principal = value; }
		}

		public ICollection<OAuthRequestException> Errors {
			get { return errors.AsReadOnly(); }
		}

		public void AddError(OAuthRequestException error) {
			errors.Add(error);
		}

		public void RemoveError(OAuthRequestException error) {
			errors.Remove(error);
		}

		public void ClearErrors() {
			errors.Clear();
		}
	}
}