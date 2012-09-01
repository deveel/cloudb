using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Net {
	public sealed class PasswordAuthenticator : IServiceAuthenticator {
		private readonly Random random;
		private readonly string password;

		public PasswordAuthenticator(string password) {
			if (password == null)
				throw new ArgumentNullException("password");

			this.password = password;
			random = new Random();
		}

		public string Password {
			get { return password; }
		}

		public bool Authenticate(AuthenticationPoint point, Stream stream) {
			BinaryReader reader = new BinaryReader(stream, Encoding.Unicode);
			BinaryWriter writer = new BinaryWriter(stream, Encoding.Unicode);

			if (point == AuthenticationPoint.Client) {
				long rv = reader.ReadInt64();

				// Send the password,
				writer.Write(rv);
				short sz = (short)Password.Length;
				writer.Write(sz);
				for (int i = 0; i < sz; ++i) {
					writer.Write(Password[i]);
				}
				writer.Flush();
				return true;
			} else {
				// Write a random long and see if it gets pinged back from the client,
				byte[] bytes = new byte[8];
				random.NextBytes(bytes);
				long rv = BitConverter.ToInt64(bytes, 0);
				writer.Write(rv);
				writer.Flush();
				long feedback = reader.ReadInt64();
				if (rv != feedback) {
					// Silently close if the value not returned,
					writer.Close();
					reader.Close();
					return false;
				}

				// Read the password string from the stream,
				short sz = reader.ReadInt16();
				StringBuilder sb = new StringBuilder(sz);
				for (int i = 0; i < sz; ++i)
					sb.Append(reader.ReadChar());

				string passwordCode = sb.ToString();
				return passwordCode.Equals(Password);
			}
		}
	}
}