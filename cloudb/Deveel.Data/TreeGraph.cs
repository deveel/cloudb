using System;
using System.Collections.Generic;

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

		public TreeGraph(string nodeName, NodeId areaRef)
			: this() {
			Init(nodeName, areaRef);
		}


		private readonly Dictionary<string, string> properties;
		private readonly List<TreeGraph> children;

		public void Init(string nodeName, long areaRef) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaRef);
		}

		public void Init(string nodeName, NodeId areaRef) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaRef.ToString());
		}

		public void SetProperty(string key, string value) {
			properties[key] = value;
		}

		public void SetProperty(string key, long value) {
			properties[key] = value.ToString();
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