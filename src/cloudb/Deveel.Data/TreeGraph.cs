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
	public class TreeGraph {
		public TreeGraph() {
			properties = new Dictionary<String, String>(4);
			children = new List<TreeGraph>(12);
		}

		public TreeGraph(string nodeName, long areaRef)
			: this() {
			Init(nodeName, areaRef);
		}


		private readonly Dictionary<string, string> properties;
		private readonly List<TreeGraph> children;

		public void Init(string nodeName, long areaRef) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaRef);
		}

		public void SetProperty(string key, string value) {
			properties[key] = value;
		}

		public void SetProperty(string key, long value) {
			properties[key] = value.ToString(CultureInfo.InvariantCulture);
		}

		public string GetProperty(string key) {
			string value;
			return properties.TryGetValue(key, out value) ? value : null;
		}

		public void AddChild(TreeGraph node) {
			children.Add(node);
		}

		public int ChildCount {
			get { return children.Count; }
		}

		public TreeGraph GetChild(int i) {
			return children[i];
		}

		public IEnumerator<TreeGraph> GetChildrenEnumerator() {
			return children.GetEnumerator();
		}

		public override string ToString() {
			return GetProperty("name") + "[" + GetProperty("ref") + "]";
		}
	}
}