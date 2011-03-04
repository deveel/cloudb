using System;

namespace Deveel.Data.Configuration {
	public interface IConfigurable {
		void Configure(ConfigSource configSource);
	}
}