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

namespace Deveel.Data.Net {
	public sealed class ServiceException : Exception {
		internal ServiceException(string source, string message, string stackTrace) {
			this.source = source;
			this.message = message;
			this.stackTrace = stackTrace;
		}

		internal ServiceException(Exception error)
			: this(error.Source, error.Message, error.StackTrace) {
		}

		private string source;
		private readonly string message;
		private readonly string stackTrace;

		public override string Message {
			get { return message == null ? String.Empty : message; }
		}

		public override string StackTrace {
			get { return stackTrace == null ? String.Empty : stackTrace; }
		}

		public override string Source {
			get { return source == null ? String.Empty : source; }
			set { source = value; }
		}
	}
}