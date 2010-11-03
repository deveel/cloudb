using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Deveel.Data.Configuration {
	public class ConfigSource : ICloneable {
		public ConfigSource()
			: this(null) {
		}

		public ConfigSource(string name) {
			this.name = name;
		}

		private ConfigSource(ConfigSource parent, string name)
			: this(name) {
			this.parent = parent;
		}

		private readonly string name;
		private readonly ConfigSource parent;
		private readonly Dictionary<string , string> keyValues = new Dictionary<string, string>();
		private readonly List<ConfigSource> children = new List<ConfigSource>();
		private string[] keys;

		public string Name {
			get { return name; }
		}

		public ConfigSource Parent {
			get { return parent; }
		}

		public string[] Keys {
			get {
				if (keys == null) {
					keys = new string[keyValues.Count];
					keyValues.Keys.CopyTo(keys, 0);
				}

				return keys;
			}
		}

		public IList<ConfigSource> Children {
			get { return children.AsReadOnly(); }
		}

		public int ChildCount {
			get { return children.Count; }
		}

		public bool HasValue(string key) {
			int index = key.IndexOf('.');
			if (index != -1) {
				string childName = key.Substring(0, index);
				ConfigSource child = GetChild(childName);
				if (child == null)
					throw new ArgumentException("The child '" + childName + "' was not found.");

				key = key.Substring(index + 1);
				return child.HasValue(key);
			}

			return keyValues.ContainsKey(key);
		}

		public T GetValue<T>(string key, T defaultValue) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			int index = key.IndexOf('.');
			if (index != -1) {
				string childName = key.Substring(0, index);
				ConfigSource child = GetChild(childName);
				if (child == null)
					throw new ArgumentException("The child '" + childName + "' was not found.");

				key = key.Substring(index + 1);
				return child.GetValue(key, defaultValue);
			}

			string value;
			if (!keyValues.TryGetValue(key, out value))
				return defaultValue;

			return (T) Convert.ChangeType(value, typeof(T));
		}

		public string GetString(string key, string defaultValue) {
			return GetValue(key, defaultValue);
		}

		public string GetString(string key) {
			return GetString(key, String.Empty);
		}

		public char GetChar(string key, char defaultValue) {
			return GetValue(key, defaultValue);
		}

		public char GetChar(string key) {
			return GetChar(key, '\0');
		}

		public short GetInt16(string key, short defaultValue) {
			return GetValue(key, defaultValue);
		}

		public short GetInt16(string key) {
			return GetInt16(key, -1);
		}

		public int GetInt32(string key, int defaultValue) {
			return GetValue(key, defaultValue);
		}

		public int GetInt32(string key) {
			return GetInt32(key, -1);
		}

		public long GetInt64(string key, long defaultValue) {
			return GetValue(key, defaultValue);
		}

		public long GetInt64(string key) {
			return GetInt64(key, -1);
		}

		public double GetFloat(string key, float defaultValue) {
			return GetValue(key, defaultValue);
		}

		public double GetFloat(string key) {
			return GetFloat(key, -1);
		}

		public double GetDouble(string key, double defaultValue) {
			return GetValue(key, defaultValue);
		}

		public double GetDouble(string key) {
			return GetDouble(key, -1);
		}

		public void SetValue<T>(string key, T value) {
			if (key == null)
				throw new ArgumentNullException("key");

			int index = key.IndexOf('.');
			if (index != -1) {
				string childName = key.Substring(0, index);
				ConfigSource child = GetChild(childName);
				if (child == null)
					child = AddChild(childName);

				key = key.Substring(index + 1);
				child.SetValue(key, value);
				return;
			}

			if (Equals(default(T), value) && keyValues.ContainsKey(key)) {
				keyValues.Remove(key);
			} else {
				keyValues[key] = Convert.ToString(value, CultureInfo.InvariantCulture);
			}
		}

		public void SetValue(string key, char value) {
			SetValue<char>(key, value);
		}

		public void SetValue(string key, string value) {
			SetValue<string>(key, value);
		}

		public void SetValue(string key, short value) {
			SetValue<short>(key, value);
		}

		public void SetValue(string key, int value) {
			SetValue<int>(key, value);
		}

		public void SetValue(string key, long value) {
			SetValue<long>(key, value);
		}

		public void SetValue(string key, float value) {
			SetValue<float>(key, value);
		}

		public void SetValue(string  key, double value) {
			SetValue<double>(key, value);
		}

		public ConfigSource AddChild(string childName) {
			if (String.IsNullOrEmpty(childName))
				throw new ArgumentNullException("childName");

			int index = childName.IndexOf('.');
			if (index != -1) {
				string parentName = childName.Substring(0, index);
				childName = childName.Substring(index + 1);

				ConfigSource configChild = GetChild(parentName);
				if (configChild == null)
					configChild = AddChild(parentName);

				return configChild.AddChild(childName);
			}

			ConfigSource child = new ConfigSource(this, childName);
			children.Add(child);
			return child;
		}

		public ConfigSource GetChild(string childName) {
			for (int i = 0; i < children.Count; i++) {
				ConfigSource child = children[i];
				if (child.Name == childName)
					return child;
			}

			return null;
		}

		public bool RemoveChild(string childName) {
			for (int i = children.Count - 1; i >= 0; i--) {
				if (children[i].Name == childName) {
					children.RemoveAt(i);
					return true;
				}
			}

			return false;
		}

		public void Load(Stream inputStream) {
			if (!inputStream.CanRead)
				throw new ArgumentException("Cannot read from the stream.", "inputStream");

			Util.Properties properties = new Util.Properties();
			properties.Load(inputStream);
			foreach(KeyValuePair<object, object> pair in properties) {
				SetValue((string)pair.Key, (string)pair.Value);
			}
		}

		public void Save(Stream outputStream) {
			if (!outputStream.CanWrite)
				throw new ArgumentException("The stream cannot be written.", "outputStream");

			Util.Properties properties = new Util.Properties();
			foreach(KeyValuePair<string, string> pair in keyValues) {
				properties.SetProperty(pair.Key, pair.Value);
			}
			properties.Store(outputStream, null);
		}

		public object Clone() {
			ConfigSource config = (ConfigSource) Activator.CreateInstance(GetType());
			foreach(KeyValuePair<string , string> pair in keyValues) {
				config.SetValue(pair.Key, pair.Value);
			}
			foreach(ConfigSource child in children) {
				config.children.Add((ConfigSource)child.Clone());
			}
			return config;
		}
	}
}