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
using System.Globalization;

namespace Deveel.Data {
	public class TreeReportNode {
		private readonly Dictionary<String, String> properties;
		private readonly List<TreeReportNode> children;

		public TreeReportNode() {
			properties = new Dictionary<string, string>(4);
			children = new List<TreeReportNode>(12);
		}

		public TreeReportNode(String nodeName, NodeId areaId)
			: this() {
			Init(nodeName, areaId);
		}

		public TreeReportNode(String nodeName, long areaId)
			: this() {
			Init(nodeName, areaId);
		}

		public void Init(String nodeName, NodeId areaId) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaId.ToString());
		}

		public void Init(String nodeName, long areaId) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaId);
		}

		public void SetProperty(string key, string value) {
			properties[key] = value;
		}

		public void SetProperty(string key, long value) {
			properties[key] = value.ToString(CultureInfo.InvariantCulture);
		}

		public String GetProperty(string key) {
			string value;
			properties.TryGetValue(key, out value);
			return value;
		}

		public ICollection<TreeReportNode> ChildNodes {
			get { return children; }
		}
	}
}