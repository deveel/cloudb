using System;
using System.Collections.Specialized;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Deveel.Data.Util {
	public static class Rfc3986 {
		private static readonly Regex Rfc3986EscapeSequence = new Regex("%([0-9A-Fa-f]{2})", RegexOptions.Compiled);

		public static string EncodeAndJoin(NameValueCollection values) {
			if (values == null)
				return string.Empty;

			StringBuilder enc = new StringBuilder();

			bool first = true;
			foreach (string key in values.Keys) {
				string encKey = Encode(key);
				foreach (string value in values.GetValues(key)) {
					if (!first)
						enc.Append("&");
					else
						first = false;

					enc.Append(encKey).Append("=").Append(Encode(value));
				}
			}

			return enc.ToString();
		}

		public static NameValueCollection SplitAndDecode(string input) {
			NameValueCollection parameters = new NameValueCollection();

			if (string.IsNullOrEmpty(input))
				return parameters;

			foreach (string pair in input.Split('&')) {
				string[] parts = pair.Split('=');

				if (parts.Length != 2)
					throw new FormatException("Pair is not a key-value pair");

				string key = Decode(parts[0]);
				if (string.IsNullOrEmpty(key))
					throw new FormatException("Key cannot be null or empty");

				string value = Decode(parts[1]);

				parameters.Add(key, value);
			}

			return parameters;
		}

		public static string Encode(string input) {
			if (string.IsNullOrEmpty(input))
				return string.Empty;

			return Encoding.ASCII.GetString(EncodeToBytes(input, Encoding.UTF8));
		}

		private static string EvaluateMatch(Match match) {
			if (match.Success) {
				Group hexgrp = match.Groups[1];

				return string.Format(CultureInfo.InvariantCulture, "{0}", (char)int.Parse(hexgrp.Value, NumberStyles.HexNumber, CultureInfo.InvariantCulture));
			}

			throw new FormatException("Could not RFC 3986 decode string");
		}

		public static string Decode(string input) {
			if (string.IsNullOrEmpty(input))
				return string.Empty;

			return Rfc3986EscapeSequence.Replace(input, EvaluateMatch);
		}

		private static byte[] EncodeToBytes(string input, Encoding enc) {
			if (string.IsNullOrEmpty(input))
				return new byte[0];

			byte[] inbytes = enc.GetBytes(input);

			// Count unsafe characters
			int unsafeChars = 0;
			char c;
			foreach (byte b in inbytes) {
				c = (char)b;

				if (NeedsEscaping(c))
					unsafeChars++;
			}

			// Check if we need to do any encoding
			if (unsafeChars == 0)
				return inbytes;

			byte[] outbytes = new byte[inbytes.Length + (unsafeChars * 2)];
			int pos = 0;

			for (int i = 0; i < inbytes.Length; i++) {
				byte b = inbytes[i];

				if (NeedsEscaping((char)b)) {
					outbytes[pos++] = (byte)'%';
					outbytes[pos++] = (byte)IntToHex((b >> 4) & 0xf);
					outbytes[pos++] = (byte)IntToHex(b & 0x0f);
				} else
					outbytes[pos++] = b;
			}

			return outbytes;
		}

		private static bool NeedsEscaping(char c) {
			return !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
					|| c == '-' || c == '_' || c == '.' || c == '~');
		}

		private static char IntToHex(int n) {
			if (n < 0 || n >= 16)
				throw new ArgumentOutOfRangeException("n");

			return n <= 9 ? (char) (n + '0') : (char) (n - 10 + 'A');
		}
	}
}