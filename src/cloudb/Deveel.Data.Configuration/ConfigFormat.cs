using System;

namespace Deveel.Data.Configuration {
	public enum ConfigFormat {
		/// <summary>
		/// A plain-text key/value configuration file
		/// </summary>
		Properties,

		/// <summary>
		/// A standalone XML-formatted configuration file.
		/// </summary>
		Xml,

		/// <summary>
		/// A section within an application configuration file.
		/// </summary>
		Configuration

		//TODO: Ini?
	}
}