using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	public class OAuthParameters : ICloneable {
		private readonly IDictionary<string, string> parameters;
		private NameValueCollection additionalParameters;

		private const string AuthorizationHeaderParameter = "Authorization";
		private const string WwwAuthenticateHeaderParameter = "WWW-Authenticate";
		private const string OAuthAuthScheme = "OAuth";

		private static readonly Regex OAuthCredentialsRegex = new Regex(@"^" + OAuthAuthScheme + @"\s+", RegexOptions.Compiled | RegexOptions.IgnoreCase);
		private static readonly Regex StringEscapeSequence = new Regex(@"\\([""'\0abfnrtv]|U[0-9a-fA-F]{8}|u[0-9a-fA-F]{4}|x[0-9a-fA-F]+)", RegexOptions.Compiled);

		/// <summary>
		/// Create a new empty OAuthParameters.
		/// </summary>
		public OAuthParameters() {
			parameters = new Dictionary<string, string>();

			Callback = null;
			ConsumerKey = null;
			Nonce = null;
			Realm = null;
			Signature = null;
			SignatureMethod = null;
			Timestamp = null;
			Token = null;
			TokenSecret = null;
			Version = null;
			Verifier = null;

			additionalParameters = new NameValueCollection();
		}

		public string Callback {
			get { return parameters[OAuthParameterKeys.Callback]; }
			set { parameters[OAuthParameterKeys.Callback] = value; }
		}

		public string ConsumerKey {
			get { return parameters[OAuthParameterKeys.ConsumerKey]; }
			set { parameters[OAuthParameterKeys.ConsumerKey] = value; }
		}

		public string Nonce {
			get { return parameters[OAuthParameterKeys.Nonce]; }
			set { parameters[OAuthParameterKeys.Nonce] = value; }
		}

		public string Realm {
			get { return parameters[OAuthParameterKeys.Realm]; }
			set { parameters[OAuthParameterKeys.Realm] = value; }
		}

		public string Signature {
			get { return parameters[OAuthParameterKeys.Signature]; }
			set { parameters[OAuthParameterKeys.Signature] = value; }
		}

		public string SignatureMethod {
			get { return parameters[OAuthParameterKeys.SignatureMethod]; }
			set { parameters[OAuthParameterKeys.SignatureMethod] = value; }
		}

		public string Timestamp {
			get { return parameters[OAuthParameterKeys.Timestamp]; }
			set { parameters[OAuthParameterKeys.Timestamp] = value; }
		}

		public string Token {
			get { return parameters[OAuthParameterKeys.Token]; }
			set { parameters[OAuthParameterKeys.Token] = value; }
		}

		public string TokenSecret {
			get { return parameters[OAuthParameterKeys.TokenSecret]; }
			set { parameters[OAuthParameterKeys.TokenSecret] = value; }
		}

		public string Version {
			get { return parameters[OAuthParameterKeys.Version]; }
			set { parameters[OAuthParameterKeys.Version] = value; }
		}

		public string Verifier {
			get { return parameters[OAuthParameterKeys.Verifier]; }
			set { parameters[OAuthParameterKeys.Verifier] = value; }
		}

		public bool HasProblem {
			get { return AdditionalParameters[OAuthErrorParameterKeys.Problem] != null; }
		}

		public NameValueCollection AdditionalParameters {
			get { return additionalParameters; }
		}

		public string ProblemAdvice {
			get { return additionalParameters[OAuthErrorParameterKeys.ProblemAdvice]; }
			set { additionalParameters[OAuthErrorParameterKeys.ProblemAdvice] = value; }
		}

		public string ProblemType {
			get { return additionalParameters[OAuthErrorParameterKeys.Problem]; }
			set { additionalParameters[OAuthErrorParameterKeys.Problem] = value; }
		}

		public string AcceptableVersions {
			get { return additionalParameters[OAuthErrorParameterKeys.AcceptableVersions]; }
			set { additionalParameters[OAuthErrorParameterKeys.AcceptableVersions] = value; }
		}

		public string ParametersAbsent {
			get { return additionalParameters[OAuthErrorParameterKeys.ParametersAbsent]; }
			set { additionalParameters[OAuthErrorParameterKeys.ParametersAbsent] = value; }
		}

		public string ParametersRejected {
			get { return additionalParameters[OAuthErrorParameterKeys.ParametersRejected]; }
			set { additionalParameters[OAuthErrorParameterKeys.ParametersAbsent] = value; }
		}

		public string AcceptableTimestamps {
			get { return additionalParameters[OAuthErrorParameterKeys.AcceptableTimestamps]; }
			set { additionalParameters[OAuthErrorParameterKeys.AcceptableTimestamps] = value; }
		}

		public static OAuthParameters Parse(IHttpRequest request) {
			return Parse(request, OAuthParameterSources.ServiceProviderDefault);
		}

		public static OAuthParameters Parse(IHttpRequest request, OAuthParameterSources sources) {
			return DoParse(request.Headers[AuthorizationHeaderParameter], request.Headers[WwwAuthenticateHeaderParameter],
			               request.Form, request.QueryString, sources, true);
		}

		public static OAuthParameters Parse(HttpWebResponse response) {
			if (response == null)
				return null;

			NameValueCollection bodyParams = new NameValueCollection();

			using (MemoryStream ms = new MemoryStream()) {
				Stream stream = response.GetResponseStream();
				byte[] buffer = new byte[32768];

				int read;

				while ((read = stream.Read(buffer, 0, buffer.Length)) > 0) {
					ms.Write(buffer, 0, read);
				}

				Encoding bodyEncoding = Encoding.ASCII;
				if (!String.IsNullOrEmpty(response.ContentEncoding))
					bodyEncoding = Encoding.GetEncoding(response.ContentEncoding);

				string responseBody = bodyEncoding.GetString(ms.ToArray());

				string[] nameValuePairs = responseBody.Split(new char[] { '&' }, StringSplitOptions.RemoveEmptyEntries);
				foreach (string nameValuePair in nameValuePairs) {
					string[] nameValuePairParts = nameValuePair.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
					if (nameValuePairParts.Length == 2)
						bodyParams.Add(HttpUtility.UrlDecode(nameValuePairParts[0]), HttpUtility.UrlDecode(nameValuePairParts[1]));
				}

				if (bodyParams.Count == 0 && responseBody.Trim().Length > 0)
					bodyParams.Add(OAuthErrorParameterKeys.Problem, responseBody);
			}

			return DoParse(null, response.Headers[WwwAuthenticateHeaderParameter], bodyParams, null,
			               OAuthParameterSources.ConsumerDefault, false);
		}

		public static OAuthParameters Parse(OAuthResource response) {
			if (response == null)
				return null;

			NameValueCollection bodyParams = new NameValueCollection();

			using (MemoryStream ms = new MemoryStream()) {
				Stream stream = response.GetResponseStream();
				byte[] buffer = new byte[32768];

				int read;

				while ((read = stream.Read(buffer, 0, buffer.Length)) > 0) {
					ms.Write(buffer, 0, read);
				}

				Encoding bodyEncoding = Encoding.ASCII;
				if (!String.IsNullOrEmpty(response.ContentEncoding))
					bodyEncoding = Encoding.GetEncoding(response.ContentEncoding);

				string responseBody = bodyEncoding.GetString(ms.ToArray());

				string[] nameValuePairs = responseBody.Split(new char[] { '&' }, StringSplitOptions.RemoveEmptyEntries);
				foreach (string nameValuePair in nameValuePairs) {
					string[] nameValuePairParts = nameValuePair.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
					if (nameValuePairParts.Length == 2)
						bodyParams.Add(HttpUtility.UrlDecode(nameValuePairParts[0]), HttpUtility.UrlDecode(nameValuePairParts[1]));
				}

				// Reset the stream
				stream.Position = 0;
			}

			return DoParse(null, response.Headers[WwwAuthenticateHeaderParameter], bodyParams, null, OAuthParameterSources.ConsumerDefault, false);
		}

		public static OAuthParameters Parse(NameValueCollection parameterCollection) {
			return DoParse(null, null, parameterCollection, null, OAuthParameterSources.PostBody, true);
		}

		public static NameValueCollection GetNonOAuthParameters(params NameValueCollection[] parameterCollections) {
			NameValueCollection @params = new NameValueCollection();

			foreach (NameValueCollection paramCollection in parameterCollections)
				if (paramCollection != null)
					foreach (string key in paramCollection.AllKeys)
						if (Array.IndexOf(OAuthParameterKeys.ReservedParameterNames, key) < 0)
							foreach (string value in paramCollection.GetValues(key))
								@params.Add(key, value);

			return @params;
		}

		public OAuthParameters Clone() {
			var clone = new OAuthParameters();

			foreach (KeyValuePair<string, string> item in parameters)
				clone.parameters[item.Key] = item.Value;

			clone.additionalParameters = new NameValueCollection(additionalParameters);

			return clone;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public void Sign(Uri requestUri, string httpMethod, IConsumer consumer, IToken token, ISignProvider signingProvider) {
			if (token != null)
				Token = token.Token;

			OAuthParameters signingParameters = Clone();
			var signingUri = new UriBuilder(requestUri);

			// Normalize the request uri for signing
			if (!string.IsNullOrEmpty(requestUri.Query)) {
				// TODO: Will the parameters necessarily be Rfc3698 encoded here? If not, then Rfc3968.SplitAndDecode will throw FormatException
				signingParameters.AdditionalParameters.Add(Rfc3986.SplitAndDecode(requestUri.Query.Substring(1)));
				signingUri.Query = null;
			}

			if (signingProvider == null)
				// There is no signing provider for this signature method
				throw new OAuthRequestException(null, OAuthProblemTypes.SignatureMethodRejected);

			// Compute the signature
			Signature = signingProvider.ComputeSignature(
				Security.Signature.Create(httpMethod, signingUri.Uri, signingParameters), consumer.Secret,
				(token != null && token.Secret != null) ? token.Secret : null);
		}

		public string ToHeader() {
			string[] excludedParameters = 
            {
                OAuthParameterKeys.Realm,
                OAuthParameterKeys.TokenSecret
            };

			StringBuilder refAuthHeader = new StringBuilder(OAuthAuthScheme);
			refAuthHeader.Append(" ");

			bool first = true;

			if (!String.IsNullOrEmpty(Realm)) {
				EncodeHeaderValue(refAuthHeader, OAuthParameterKeys.Realm, Realm, first ? string.Empty : ", ", true);
				first = false;
			}

			foreach (string key in parameters.Keys) {
				if (parameters[key] != null && Array.IndexOf(excludedParameters, key) < 0) {
					EncodeHeaderValue(refAuthHeader, key, parameters[key], first ? string.Empty : ", ", true);
					first = false;
				}
			}

			return refAuthHeader.ToString();
		}

		public string ToQueryString() {
			string[] excludedParameters = 
            {
                OAuthParameterKeys.Realm,
                OAuthParameterKeys.TokenSecret
            };

			StringBuilder queryString = new StringBuilder();

			bool first = true;

			foreach (string key in parameters.Keys) {
				if (parameters[key] != null && Array.IndexOf(excludedParameters, key) < 0) {
					EncodeHeaderValue(queryString, key, parameters[key], first ? String.Empty : "&", false);
					first = false;
				}
			}

			foreach (string key in additionalParameters.Keys) {
				string[] values = additionalParameters.GetValues(key);
				if (values == null || values.Length == 0)
					continue;

				foreach (string value in values) {
					EncodeHeaderValue(queryString, key, value, first ? string.Empty : "&", false);
					first = false;
				}
			}

			return queryString.ToString();
		}

		public void RequireAllOf(params string[] requiredParameters) {
			List<string> missing = new List<string>();

			foreach (string requiredParameter in requiredParameters)
				if (string.IsNullOrEmpty(parameters[requiredParameter]))
					missing.Add(requiredParameter);

			if (missing.Count > 0)
				throw new ParametersAbsentException(null, missing.ToArray());
		}

		public void AllowOnly(params string[] allowedParameters) {
			List<string> invalid = new List<string>();

			foreach (var parameter in parameters.Keys)
				if (!String.IsNullOrEmpty(parameters[parameter]))
					if (Array.IndexOf(allowedParameters, parameter) < 0)
						invalid.Add(parameter);

			foreach (var parameter in AdditionalParameters.AllKeys)
				if (!string.IsNullOrEmpty(AdditionalParameters[parameter]))
					if (Array.IndexOf(allowedParameters, parameter) < 0)
						invalid.Add(parameter);

			if (invalid.Count > 0)
				throw new ParametersRejectedException(null, invalid.ToArray());
		}

		public void RequireVersion(params string[] allowedVersions) {
			if (allowedVersions == null)
				throw new ArgumentNullException("allowedVersions");

			if (allowedVersions.Length < 1)
				throw new ArgumentException("allowedVersions argument is mandatory", "allowedVersions");

			if (!String.IsNullOrEmpty(parameters[OAuthParameterKeys.Version]))
				foreach (string allowedVersion in allowedVersions)
					if (allowedVersion.Equals(parameters[OAuthParameterKeys.Version]))
						return;

			throw new VersionRejectedException(null, allowedVersions[0], allowedVersions[allowedVersions.Length - 1]);
		}

		public NameValueCollection OAuthRequestParams() {
			////We don't send the realm or token secret in the querystring or post body.
			return OAuthRequestParams(OAuthParameterKeys.Realm, OAuthParameterKeys.TokenSecret);
		}

		private static int CompareKeys(KeyValuePair<string,string> right, KeyValuePair<string, string> left) {
			return left.Key.Equals(right.Key, StringComparison.Ordinal)
			       	? String.Compare(left.Value, right.Value, StringComparison.Ordinal)
			       	: String.Compare(left.Key, right.Key, StringComparison.Ordinal);
		}

		public string ToNormalizedString(params string[] excludedParameters) {
			List<KeyValuePair<string,string>> @params = new List<KeyValuePair<string, string>>();

			// Add OAuth parameters whose values are not null except excluded parameters
			foreach (string param in parameters.Keys)
				if (parameters[param] != null && Array.IndexOf(excludedParameters, param) < 0)
					@params.Add(new KeyValuePair<string, string>(Rfc3986.Encode(param), Rfc3986.Encode(parameters[param])));

			// Add all additional parameters
			foreach (var param in additionalParameters.AllKeys)
				foreach (var value in additionalParameters.GetValues(param) ?? new string[] { })
					@params.Add(new KeyValuePair<string, string>(Rfc3986.Encode(param), Rfc3986.Encode(value)));

			// Sort parameters into lexicographic order (by key and value)
			@params.Sort(CompareKeys);

			// Concatenate and encode
			string equals = "=";
			string ampersand = "&";

			StringBuilder parms = new StringBuilder();
			bool first = true;
			foreach (var pair in @params) {
				if (first)
					first = false;
				else
					parms.Append(ampersand);

				parms.Append(pair.Key).Append(equals).Append(pair.Value);
			}

			return parms.ToString();
		}

		internal static OAuthParameters DoParse(string authHeader, string wwwAuthHeader, NameValueCollection form, NameValueCollection queryString, OAuthParameterSources sources, bool validateParameters) {
			if (sources == OAuthParameterSources.None)
				throw new ArgumentException("sources must not be OAuthParameterSources.None", "sources");

			bool useAuthHeader = (sources & OAuthParameterSources.AuthorizationHeader) == OAuthParameterSources.AuthorizationHeader;
			bool useWwwAuthHeader = (sources & OAuthParameterSources.AuthenticateHeader) == OAuthParameterSources.AuthenticateHeader;
			bool usePost = (sources & OAuthParameterSources.PostBody) == OAuthParameterSources.PostBody;
			bool useQueryString = (sources & OAuthParameterSources.QueryString) == OAuthParameterSources.QueryString;

			NameValueCollection authHeaderParams = useAuthHeader ? ParseAuthHeader(authHeader) : null;
			NameValueCollection wwwAuthHeaderParams = useWwwAuthHeader ? ParseAuthHeader(wwwAuthHeader) : null;
			NameValueCollection postParams = usePost ? form : null;
			NameValueCollection queryStringParams = useQueryString ? queryString : null;

			// Do validation if required
			if (validateParameters) {
				/*
				 * Check for any duplicated OAuth parameters
				 */
				ResultInfo<string[]> result = CheckForDuplicateReservedParameters(authHeaderParams, wwwAuthHeaderParams, postParams, queryStringParams);

				if (!result)
					throw new ParametersRejectedException(null, result);

				/*
				 * Check for non-reserved parameters prefixed with oauth_
				 */
				result = CheckForInvalidParameterNames(authHeaderParams, wwwAuthHeaderParams, postParams, queryStringParams);

				if (!result)
					throw new ParametersRejectedException(null, result);
			}

			OAuthParameters parameters = new OAuthParameters();
			parameters.Callback = GetParam(OAuthParameterKeys.Callback, authHeaderParams, wwwAuthHeaderParams, postParams,
			                               queryStringParams);
			parameters.ConsumerKey = GetParam(OAuthParameterKeys.ConsumerKey, authHeaderParams, wwwAuthHeaderParams, postParams,
			                                  queryStringParams);
			parameters.Nonce = GetParam(OAuthParameterKeys.Nonce, authHeaderParams, postParams, wwwAuthHeaderParams,
			                            queryStringParams);
			parameters.Realm = authHeaderParams != null ? authHeaderParams[OAuthParameterKeys.Realm] : null;
			parameters.Signature = GetParam(OAuthParameterKeys.Signature, authHeaderParams, wwwAuthHeaderParams, postParams,
			                                queryStringParams);
			parameters.SignatureMethod = GetParam(OAuthParameterKeys.SignatureMethod, wwwAuthHeaderParams, authHeaderParams,
			                                      postParams, queryStringParams);
			parameters.Timestamp = GetParam(OAuthParameterKeys.Timestamp, authHeaderParams, wwwAuthHeaderParams, postParams,
			                                queryStringParams);
			parameters.Token = GetParam(OAuthParameterKeys.Token, authHeaderParams, wwwAuthHeaderParams, postParams,
			                            queryStringParams);
			parameters.TokenSecret = GetParam(OAuthParameterKeys.TokenSecret, authHeaderParams, wwwAuthHeaderParams, postParams,
			                                  queryStringParams);
			parameters.Version = GetParam(OAuthParameterKeys.Version, authHeaderParams, wwwAuthHeaderParams, postParams,
			                              queryStringParams);
			parameters.Verifier = GetParam(OAuthParameterKeys.Verifier, authHeaderParams, wwwAuthHeaderParams, postParams,
			                               queryStringParams);

			parameters.additionalParameters = GetNonOAuthParameters(wwwAuthHeaderParams, postParams, queryStringParams);
			return parameters;
		}

		private static void EncodeHeaderValue(StringBuilder buffer, string key, string value, string separator, bool quote) {
			buffer.Append(separator);
			buffer.Append(Rfc3986.Encode(key));
			buffer.Append("=");

			if (quote)
				buffer.Append('"');

			buffer.Append(Rfc3986.Encode(value));

			if (quote)
				buffer.Append('"');
		}

		private static string EvaluateAuthHeaderMatch(Match match) {
			Group group = match.Groups[1];
			if (group.Length == 1) {
				switch (group.Value) {
					case "\"": return "\"";
					case "'": return "'";
					case "\\": return "\\";
					case "0": return "\0";
					case "a": return "\a";
					case "b": return "\b";
					case "f": return "\f";
					case "n": return "\n";
					case "r": return "\r";
					case "t": return "\t";
					case "v": return "\v";
				}
			}

			return String.Format(CultureInfo.InvariantCulture, "{0}", char.Parse(group.Value));

		}

		private static NameValueCollection ParseAuthHeader(string authHeader) {
			if (!String.IsNullOrEmpty(authHeader)) {
				NameValueCollection @params = new NameValueCollection();

				// Check for OAuth auth-scheme
				Match authSchemeMatch = OAuthCredentialsRegex.Match(authHeader);
				if (authSchemeMatch.Success) {
					// We have OAuth credentials in the Authorization header; parse the parts
					// Sad-to-say, but this code is much simpler than the regex for it!
					string[] authParameterValuePairs = authHeader.Substring(authSchemeMatch.Length).Split(',');

					foreach (string authParameterValuePair in authParameterValuePairs) {
						string[] parts = authParameterValuePair.Trim().Split('=');

						if (parts.Length == 2) {
							string parameter = parts[0];
							string value = parts[1];

							if (value.StartsWith("\"", StringComparison.Ordinal) && value.EndsWith("\"", StringComparison.Ordinal)) {
								value = value.Substring(1, value.Length - 2);

								try {
									value = StringEscapeSequence.Replace(value, EvaluateAuthHeaderMatch);
								} catch (FormatException) {
									continue;
								}

								// Add the parameter and value
								@params.Add(Rfc3986.Decode(parameter), Rfc3986.Decode(value));
							}
						}
					}
				}

				return @params;
		}

			return null;
		}

		private static ResultInfo<string[]> CheckForDuplicateReservedParameters(params NameValueCollection[] paramCollections) {
			List<string> duplicated = new List<string>();
			int count;

			foreach (string param in OAuthParameterKeys.ReservedParameterNames) {
				count = 0;

				foreach (NameValueCollection paramCollection in paramCollections)
					if (paramCollection != null) {
						string[] values = paramCollection.GetValues(param);
						if (values != null)
							count += values.Length;
					}

				if (count > 1)
					duplicated.Add(param);
			}

			return duplicated.Count > 0
				? new ResultInfo<string[]>(false, duplicated.ToArray())
				: new ResultInfo<string[]>(true, null);
		}

		private static ResultInfo<string[]> CheckForInvalidParameterNames(
				params NameValueCollection[] paramCollections) {
			List<string> invalid = new List<string>();

			foreach (NameValueCollection paramCollection in paramCollections)
				if (paramCollection != null)
					foreach (string param in paramCollection.Keys) {
						if (param != null && 
							param.StartsWith(OAuthParameterKeys.OAuthParameterPrefix, StringComparison.Ordinal) && 
							Array.IndexOf(OAuthParameterKeys.ReservedParameterNames, param) < 0) {
							invalid.Add(param);
						}
					}

			return invalid.Count > 0
				? new ResultInfo<string[]>(false, invalid.ToArray())
				: new ResultInfo<string[]>(true, null);
		}

		private static string GetParam(string param, params NameValueCollection[] paramCollections) {
			foreach (NameValueCollection paramCollection in paramCollections)
				if (paramCollection != null && !string.IsNullOrEmpty(paramCollection[param]))
					return paramCollection[param];

			return null;
		}

		private NameValueCollection OAuthRequestParams(params string[] excludedParameters) {
			NameValueCollection @params = new NameValueCollection();

			// Add OAuth parameters whose values are not null except excluded parameters
			foreach (string param in parameters.Keys)
				if (parameters[param] != null && Array.IndexOf(excludedParameters, param) < 0)
					@params.Add(param, parameters[param]);

			return @params;
		}
	}
}