using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Net.Client {
	public sealed class PathStruct {
		private readonly Dictionary<string, PathValue> members;

		public PathStruct() {
			members = new Dictionary<string, PathValue>();
		}

		public int MemberCount {
			get { return members.Count; }
		}

		public PathValue this[string memberName] {
			get { return GetValue(memberName); }
			set { SetValue(memberName, value); }
		}

		internal string[] MemberNames {
			get { 
				string[] memberNames = new string[members.Count];
				members.Keys.CopyTo(memberNames, 0);
				return memberNames;
			}
		}

		public bool HasMember(string memberName) {
			return members.ContainsKey(memberName);
		}

		public PathValue GetValue(string memberName) {
			PathValue value;
			if (!members.TryGetValue(memberName, out value))
				return PathValue.Null;
			return value;
		}

		public void SetValue(string memberName, PathValue value) {
			members[memberName] = value;
		}

		public static PathStruct FromObject(object obj) {
			if (obj == null)
				return null;

			Type type = obj.GetType();
			MemberInfo[] memberInfos = type.FindMembers(MemberTypes.Field | MemberTypes.Property, BindingFlags.Public, null, null);

			int sz = memberInfos.Length;
			if (sz == 0)
				return new PathStruct();

			PathStruct pathStruct = new PathStruct();

			for (int i = 0; i < sz; i++) {
				MemberInfo memberInfo = memberInfos[i];

				string memberName = memberInfo.Name;
				object value;
				if (memberInfo is FieldInfo) {
					value = ((FieldInfo)memberInfo).GetValue(obj);
				} else {
					value = ((PropertyInfo)memberInfo).GetValue(obj, null);
				}

				pathStruct.SetValue(memberName, new PathValue(value));
			}

			return pathStruct;
		}

		public object ToObject(Type type) {
			object obj = Activator.CreateInstance(type, true);

			MemberInfo[] memberInfos = type.FindMembers(MemberTypes.Field | MemberTypes.Property, BindingFlags.Public, null, null);

			int sz = memberInfos.Length;
			if (sz == 0)
				return obj;

			for (int i = 0; i < sz; i++) {
				MemberInfo memberInfo = memberInfos[i];

				if (!HasMember(memberInfo.Name))
					continue;

				Type memberType;
				if (memberInfo is FieldInfo) {
					memberType = ((FieldInfo) memberInfo).FieldType;
				} else {
					memberType = ((PropertyInfo) memberInfo).PropertyType;
				}

				PathValue pathValue = GetValue(memberInfo.Name);
				object value = Convert.ChangeType(pathValue, memberType);

				if (memberInfo is FieldInfo) {
					((FieldInfo)memberInfo).SetValue(obj, value);
				} else {
					((PropertyInfo)memberInfo).SetValue(obj, value, null);
				}
			}

			return obj;
		}
	}
}