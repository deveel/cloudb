using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Configuration {
	public sealed class PropertiesConfigFormatter : IConfigFormatter {
		private static void SetChildValues(Util.Properties properties, string prefix, ConfigSource config) {
			prefix += config.Name;
			
			foreach(string key in config.Keys) {
				string value = config.GetString(key, null);
				if (String.IsNullOrEmpty(value))
					continue;
				
				properties.SetProperty(prefix + "." + key, value);
			}
			
			foreach(ConfigSource child in config.Children) {
				SetChildValues(properties, prefix, child);
			}
		}
		
		public void Load(ConfigSource config, Stream input) {
			if (!input.CanRead)
				throw new ArgumentException("Cannot read from the stream.", "input");
			
			Util.Properties properties = new Util.Properties();
			properties.Load(input);
			foreach(KeyValuePair<object, object> pair in properties) {
				config.SetValue((string)pair.Key, (string)pair.Value);
			}		
		}
		
		public void Save(ConfigSource config, Stream output) {
			if (!output.CanWrite)
				throw new ArgumentException("The output stream cannot be written.", "output");
			
			Util.Properties properties = new Util.Properties();
			SetChildValues(properties, "", config);			
			properties.Store(output, null);
		}
	}
}