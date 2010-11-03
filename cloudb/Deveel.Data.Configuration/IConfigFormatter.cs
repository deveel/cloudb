using System;
using System.IO;

namespace Deveel.Data.Configuration {
	public interface IConfigFormatter {
		void Load(ConfigSource config, Stream input);
		
		void Save(ConfigSource config, Stream output);
	}
}