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