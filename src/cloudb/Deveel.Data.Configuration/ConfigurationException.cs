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