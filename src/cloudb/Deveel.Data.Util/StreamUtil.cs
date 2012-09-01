using System;
using System.IO;

namespace Deveel.Data.Util {
	static class StreamUtil {
		public static void Copy(Stream input, Stream output) {
			Copy(input, output, 1024);
		}

		public static void Copy(Stream input, Stream output, int bufferSize) {
		 	byte[] copyBuffer = new byte[bufferSize];
			int readCount;
			while ((readCount = input.Read(copyBuffer, 0, bufferSize)) != 0) {
				output.Write(copyBuffer, 0, readCount);
			}
		}

		public static byte[] AsBuffer(Stream stream) {
			MemoryStream copyStream = new MemoryStream();
			Copy(stream, copyStream);
			copyStream.Flush();
			return copyStream.ToArray();
		}
	}
}