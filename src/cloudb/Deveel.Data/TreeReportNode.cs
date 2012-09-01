using System;
using System.Collections.Generic;

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
			properties[key] = value.ToString();
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