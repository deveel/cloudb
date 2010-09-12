//  
//  ILogger.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// An interface for logging errors, warnings, messages, and exceptions 
	/// in the system.
	/// </summary>
	/// <remarks>
	/// The implementation of where the log is written (to the console, file, 
	/// window, etc) is implementation defined.
	/// </remarks>
	public interface ILogger : IDisposable {
		/// <summary>
		/// Initialize the logger instance with the configuration
		/// properties specified.
		/// </summary>
		/// <param name="config">The configurations used to configure
		/// the logger.</param>
		void Init(ConfigSource config);

		/// <summary>
		/// Queries the current debug level.
		/// </summary>
		/// <param name="level"></param>
		/// <remarks>
		/// This can be used to speed up certain complex debug displaying 
		/// operations where the debug listener isn't interested in the 
		/// information be presented.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the debug listener is interested in debug 
		/// information of this given level.
		/// </returns>
		bool IsInterestedIn(LogLevel level);

		void Log(LogEntry entry);
	}
}