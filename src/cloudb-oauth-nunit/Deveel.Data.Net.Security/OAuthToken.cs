using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

using Deveel.Data.Util;

namespace Deveel.Data.Net.Security {
	[Serializable]
	public class OAuthToken : IToken, ISerializable {
		private readonly TokenType type;
		private readonly string token;
		private readonly string secret;
		private readonly string consumerKey;

		public OAuthToken(TokenType type, string token, string secret, IConsumer consumer)
			: this(type, token, secret, consumer.Key) {
		}

		public OAuthToken(TokenType type, string token, string secret, string consumerKey) {
			this.type = type;
			this.token = token;
			this.secret = secret;
			this.consumerKey = consumerKey;
		}

		protected OAuthToken(SerializationInfo info, StreamingContext context) {
			OAuthToken t = Deserialize(info.GetString("serializedForm"));

			type = t.Type;
			token = t.Token;
			secret = t.Secret;
			consumerKey = t.ConsumerKey;
		}

		public TokenType Type {
			get { return type; }
		}

		public string Token {
			get { return token; }
		}

		public string Secret {
			get { return secret; }
		}

		public string ConsumerKey {
			get { return consumerKey; }
		}

		public static string Serialize(OAuthToken token) {
			if (token == null)
				throw new ArgumentNullException("token");

			return "[" + Rfc3986.Encode(Enum.Format(typeof(TokenType), token.Type, "G"))
				+ "|" + Rfc3986.Encode(token.Token)
				+ "|" + Rfc3986.Encode(token.Secret)
				+ "|" + Rfc3986.Encode(token.ConsumerKey)
				+ "]";
		}

		public static OAuthToken Deserialize(string serializedForm) {
			if (string.IsNullOrEmpty(serializedForm))
				throw new ArgumentException("serializedForm argument must not be null or empty", "serializedForm");

			if (!serializedForm.StartsWith("[", StringComparison.Ordinal))
				throw new FormatException("Serialized SimpleToken must start with [");

			if (!serializedForm.EndsWith("]", StringComparison.Ordinal))
				throw new FormatException("Serialized SimpleToken must end with ]");

			string[] parts = serializedForm.Substring(1, serializedForm.Length - 2)
				.Split(new char[] { '|' }, StringSplitOptions.None);

			if (parts.Length != 4)
				throw new FormatException("Serialized SimpleToken must consist of 4 pipe-separated fields");

			if (string.IsNullOrEmpty(parts[0]))
				throw new FormatException("Error deserializing SimpleToken.Type (field 0): cannot be null or empty");

			TokenType type;
			try {
				type = (TokenType)Enum.Parse(typeof(TokenType), Rfc3986.Decode(parts[0]), true);
			} catch (Exception e) {
				throw new FormatException("Error deserializing SimpleToken.Type (field 0)", e);
			}

			if (string.IsNullOrEmpty(parts[1]))
				throw new FormatException("Error deserializing SimpleToken.Token (field 1): cannot be null or empty");

			string token;
			try {
				token = Rfc3986.Decode(parts[1]);
			} catch (Exception e) {
				throw new FormatException("Error deserializing SimpleToken.Token (field 1)", e);
			}

			string secret;
			try {
				secret = Rfc3986.Decode(parts[2]);
			} catch (Exception e) {
				throw new FormatException("Error deserializing SimpleToken.Secret (field 2)", e);
			}

			string consumerKey;
			try {
				consumerKey = Rfc3986.Decode(parts[3]);
			} catch (Exception e) {
				throw new FormatException("Error deserializing SimpleToken.ConsumerKey (field 3)", e);
			}

			return new OAuthToken(type, token, secret, consumerKey);
		}

		[SecurityPermission(SecurityAction.LinkDemand, Flags = SecurityPermissionFlag.SerializationFormatter)]
		public virtual void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("serializedForm", Serialize(this));
		}

		public override int GetHashCode() {
			int hash = type.GetHashCode();

			if (token != null)
				hash ^= token.GetHashCode();

			if (secret != null)
				hash ^= secret.GetHashCode();

			if (consumerKey != null)
				hash ^= consumerKey.GetHashCode();

			return hash;
		}

		public override bool Equals(object obj) {
			if (ReferenceEquals(this, obj))
				return true;

			OAuthToken other = obj as OAuthToken;

			if (other == null)
				return false;

			return Equals(other);
		}

		private bool Equals(IToken other) {
			return other != null && type == other.Type && String.Equals(token, other.Token) &&
			       String.Equals(secret, other.Secret) && String.Equals(consumerKey, other.ConsumerKey);
		}
	}
}