﻿//
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
	[AttributeUsage(AttributeTargets.Class, AllowMultiple=false)]
	public sealed class PathNameAttribute : Attribute {
		private string name;
		private string description;
		
		public PathNameAttribute(string name, string description) {
			this.name = name;
			this.description = description;
		}
		
		public PathNameAttribute(string name)
			: this(name, null) {
		}
		
		public PathNameAttribute()
			: this(null) {
		}
		
		public string Description {
			get { return description; }
			set { description = value; }
		}
		
		public string Name {
			get { return name; }
			set { name = value; }
		}
	}
}