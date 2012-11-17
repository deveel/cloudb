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

namespace Deveel.Data.Configuration {
	public class ConfigurationException : ApplicationException {
		private readonly ConfigSource config;
		private readonly string key;

		public ConfigurationException() {
		}

		public ConfigurationException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public ConfigurationException(string message)
			: base(message) {
		}

		public  ConfigurationException(string message, ConfigSource config, string key)
			: this(message) {
			this.config = config;
			this.key = key;
		}

		public ConfigurationException(string message, ConfigSource config)
			: this(message, config, null) {
		}

		public ConfigurationException(ConfigSource config, string key)
			: this(CreateMessage(config, key), config, key) {
		}

		public ConfigurationException(ConfigSource config)
			: this(CreateMessage(config, null)) {
		}

		public string Key {
			get { return key; }
		}

		public ConfigSource Config {
			get { return config; }
		}

		private static string CreateMessage(ConfigSource config, string key) {
			string message;

			if (!String.IsNullOrEmpty(key)) {
				message = "A configuration error occurred at source '" + config.FullName + "' on key '" + key + "'.";
			} else {
				message = "A configuration error occurred at source '" + config.FullName + "'";
			}

			return message;
		}
	}
}