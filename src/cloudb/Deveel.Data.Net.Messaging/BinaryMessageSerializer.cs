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
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Net.Messaging {
	public abstract class BinaryMessageSerializer : IMessageSerializer {
		private Encoding encoding;

		protected BinaryMessageSerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		protected BinaryMessageSerializer()
			: this(null) {
		}

		public Encoding Encoding {
			get { return encoding ?? (encoding = Encoding.Unicode); }
			set { encoding = value; }
		}

		public void Serialize(IEnumerable<Message> message, Stream output) {
			if (output == null)
				throw new ArgumentNullException("output");

			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.");

			BinaryWriter writer = new BinaryWriter(output, Encoding);
			Serialize(message, writer);
		}

		protected abstract void Serialize(IEnumerable<Message> message, BinaryWriter writer);

		public IEnumerable<Message> Deserialize(Stream input) {
			if (!input.CanRead)
				throw new ArgumentException("The inpuit stream cannot be read.");

			BinaryReader reader = new BinaryReader(input, Encoding);
			return Deserialize(reader);
		}

		protected abstract IEnumerable<Message> Deserialize(BinaryReader input);

	}
}