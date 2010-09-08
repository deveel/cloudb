using System;

namespace Deveel.Data.Util {
	interface IInputStream {
		int Available { get; }
		
		bool MarkSupported { get; }
		
		
		long Skip(long toskip);
		
		void Mark(int readLimit);
		
		void Reset();
	}
}

