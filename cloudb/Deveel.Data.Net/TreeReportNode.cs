using System;
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public class TreeReportNode {
		public TreeReportNode() {
			properties = new Dictionary<String, String>(4);
			children = new List<TreeReportNode>(12);
		}

		public TreeReportNode(string nodeName, long areaRef)
			: this() {
			Init(nodeName, areaRef);
		}


		private readonly Dictionary<string, string> properties;
		private readonly List<TreeReportNode> children;

		public void Init(string nodeName, long areaRef) {
			SetProperty("name", nodeName);
			SetProperty("ref", areaRef);
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

		public void AddChild(TreeReportNode node) {
			children.Add(node);
		}

		public int ChildCount {
			get { return children.Count; }
		}

		public TreeReportNode GetChild(int i) {
			return children[i];
		}

		public IEnumerator<TreeReportNode> GetChildrenEnumerator() {
			return children.GetEnumerator();
		}

		public override string ToString() {
			return GetProperty("name") + "[" + GetProperty("ref") + "]";
		}
	}
}