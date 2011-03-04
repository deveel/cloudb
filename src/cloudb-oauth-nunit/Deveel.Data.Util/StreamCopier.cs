using System;
using System.IO;

namespace Deveel.Data.Util {
	internal static class StreamCopier {
		private const long DefaultStreamChunkSize = 0x1000;

		public static void CopyTo(Stream from, Stream to) {
			if (!from.CanRead || !to.CanWrite) {
				return;
			}

			byte [] buffer = from.CanSeek ? new byte[from.Length] : new byte[DefaultStreamChunkSize];
			int read;

			while ((read = from.Read(buffer, 0, buffer.Length)) > 0) {
				to.Write(buffer, 0, read);
			}
		}
	}
}