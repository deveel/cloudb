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

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
	public sealed class HandleAttribute : Attribute {
		private readonly string pathTypeName;

		public HandleAttribute(string pathTypeName) {
			this.pathTypeName = pathTypeName;
		}

		public HandleAttribute(Type pathType)
			: this(pathType.FullName + ", " + pathType.Assembly.GetName().Name) {
		}

		public string PathTypeName {
			get { return pathTypeName; }
		}
	}
}