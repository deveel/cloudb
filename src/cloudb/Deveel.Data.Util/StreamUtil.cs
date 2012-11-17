//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

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