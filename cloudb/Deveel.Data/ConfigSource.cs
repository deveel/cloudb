using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Deveel.Data {
	public class ConfigSource : ICloneable {
		public ConfigSource() {
		}

		public ConfigSource(Stream inputStream) {
			Load(inputStream);
		}

		private readonly Dictionary<string , string> keyValues = new Dictionary<string, string>();
		private string[] keys;
		private bool dirty;

		public string[] Keys {
			get {
				if (keys == null || dirty) {
					keys = new string[keyValues.Count];
					keyValues.Keys.CopyTo(keys, 0);
				}

				return keys;
			}
		}

		public bool HasValue(string key) {
			return keyValues.ContainsKey(key);
		}

		public T GetValue<T>(string  key, T defaultValue) {
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

			if (Equals(default(T), value) && keyValues.ContainsKey(key)) {
				keyValues.Remove(key);
			} else {
				keyValues[key] = Convert.ToString(value, CultureInfo.InvariantCulture);
			}

			dirty = true;
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
			return config;
		}
	}
}