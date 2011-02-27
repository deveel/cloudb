using System;
using System.Collections;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Deveel.Data.Net.Security {
	public sealed class PublicKey {
		public PublicKey(string xmlString, int dwKeySize) {
			rsa = new RSACryptoServiceProvider(dwKeySize);
			rsa.FromXmlString(xmlString);
			this.dwKeySize = dwKeySize;
		}

		public PublicKey(string xmlString)
			: this(xmlString, 1024) {
		}

		private readonly int dwKeySize;
		private readonly RSACryptoServiceProvider rsa;

		public int KeySize {
			get { return dwKeySize; }
		}

		public string EncryptString(string inputString) {
			// TODO: Add Proper Exception Handlers
			int keySize = dwKeySize / 8;
			byte[] bytes = Encoding.UTF32.GetBytes(inputString);
			// The hash function in use by the .NET RSACryptoServiceProvider here 
			// is SHA1
			// int maxLength = ( keySize ) - 2 - 
			//              ( 2 * SHA1.Create().ComputeHash( rawBytes ).Length );
			int maxLength = keySize - 42;
			int dataLength = bytes.Length;
			int iterations = dataLength / maxLength;
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i <= iterations; i++) {
				byte[] tempBytes = new byte[(dataLength - maxLength*i > maxLength) ? maxLength : dataLength - maxLength*i];
				Buffer.BlockCopy(bytes, maxLength*i, tempBytes, 0, tempBytes.Length);
				byte[] encryptedBytes = rsa.Encrypt(tempBytes, true);
				// Be aware the RSACryptoServiceProvider reverses the order of 
				// encrypted bytes. It does this after encryption and before 
				// decryption. If you do not require compatibility with Microsoft 
				// Cryptographic API (CAPI) and/or other vendors. Comment out the 
				// next line and the corresponding one in the DecryptString function.
				Array.Reverse(encryptedBytes);
				// Why convert to base 64?
				// Because it is the largest power-of-two base printable using only 
				// ASCII characters
				stringBuilder.Append(Convert.ToBase64String(encryptedBytes));
			}
			return stringBuilder.ToString();
		}

		public string DecryptString(string inputString) {
			// TODO: Add Proper Exception Handlers
			int base64BlockSize = ((dwKeySize / 8) % 3 != 0) ?
			  (((dwKeySize / 8) / 3) * 4) + 4 : ((dwKeySize / 8) / 3) * 4;
			int iterations = inputString.Length / base64BlockSize;
			ArrayList arrayList = new ArrayList();
			for (int i = 0; i < iterations; i++) {
				byte[] encryptedBytes = Convert.FromBase64String(
					 inputString.Substring(base64BlockSize * i, base64BlockSize));
				// Be aware the RSACryptoServiceProvider reverses the order of 
				// encrypted bytes after encryption and before decryption.
				// If you do not require compatibility with Microsoft Cryptographic 
				// API (CAPI) and/or other vendors.
				// Comment out the next line and the corresponding one in the 
				// EncryptString function.
				Array.Reverse(encryptedBytes);
				arrayList.AddRange(rsa.Decrypt(encryptedBytes, true));
			}
			return Encoding.UTF32.GetString((byte[]) arrayList.ToArray(typeof(byte)));
		}

		public static PublicKey GenerateKey(int keySize) {
			RSACryptoServiceProvider rsa = new RSACryptoServiceProvider(keySize);
			string xmlString = rsa.ToXmlString(false);
			return new PublicKey(xmlString, keySize);
		}

		public override string ToString() {
			return rsa.ToXmlString(false);
		}

		public void SaveToFile(string filePath) {
			string xmlString = ToString();
			FileStream fileStream = null;

			try {
				fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None);
				StreamWriter writer = new StreamWriter(fileStream);
				writer.Write(xmlString);
				writer.Flush();
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
		}

		public static PublicKey LoadFromFile(string filePath) {
			FileStream fileStream = null;

			try {
				fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
				StreamReader reader = new StreamReader(fileStream);

				StringBuilder sb = new StringBuilder();
				string line;
				while ((line = reader.ReadLine()) != null)
					sb.Append(line);
				return new PublicKey(sb.ToString());
			} finally {
				if (fileStream != null)
					fileStream.Close();
			}
		}
	}
}