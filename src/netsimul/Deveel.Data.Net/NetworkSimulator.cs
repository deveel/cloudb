using System;

using Deveel.Configuration;

namespace Deveel.Data.Net {
	public sealed class NetworkSimulator {
		private static Options GetOptions() {
			Options options = new Options();
			options.AddOption("c", "conf", true, "A file containing the configurations to make the " + 
			                  "instance of the network simulator.");
			options.AddOption("");
			return options;
		}
		
		public static int Main(string[] args) {
			
			return 0;
		}
	}
}