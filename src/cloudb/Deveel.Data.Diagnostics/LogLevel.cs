//  
//  LogLevel.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	///<summary>
	/// Debug level static values.
	///</summary>
	public sealed class LogLevel {
		private LogLevel(string name, int value) {
			this.name = name;
			this.value = value;
		}

		private readonly int value;
		private readonly string name;

		///<summary>
		/// General processing 'noise'
		///</summary>
		public static readonly LogLevel Information = new LogLevel("INFO", 100);

		///<summary>
		/// Query information 'noise'
		///</summary>
		public static readonly LogLevel QueryInformation = new LogLevel("QUERY_INFO", 101);

		///<summary>
		/// A message of some importance
		///</summary>
		public static readonly LogLevel Warning = new LogLevel("WARN", 20);

		///<summary>
		/// Crackers, etc
		///</summary>
		public static readonly LogLevel Alert = new LogLevel("ALERT", 30);

		///<summary>
		/// Errors, exceptions
		///</summary>
		public static readonly LogLevel Error = new LogLevel("ERROR", 100);

		///<summary>
		/// Always printed messages (not error's however)
		///</summary>
		public static readonly LogLevel Message = new LogLevel("MESSAGE", 10000);

		public string Name {
			get { return name; }
		}

		public int Value {
			get { return value; }
		}

		public override bool Equals(object obj) {
			LogLevel level = obj as LogLevel;
			return level != null && value == level.value;
		}

		public override int GetHashCode() {
			return value.GetHashCode();
		}

		public override string ToString() {
			return name;
		}

		internal static LogLevel Create(string name, int value) {
			return new LogLevel(name, value);
		}

		public static bool operator >(LogLevel a, LogLevel b) {
			return a.value > b.value;
		}

		public static bool operator >(LogLevel a, int value) {
			return a.value > value;
		}

		public static bool operator <(LogLevel a, LogLevel b) {
			return a.value < b.value;
		}

		public static bool operator <(LogLevel a, int value) {
			return a.value < value;
		}

		public static bool operator >=(LogLevel a, LogLevel b) {
			return a.value >= b.value;
		}

		public static bool operator >=(LogLevel a, int value) {
			return a.value >= value;
		}

		public static bool operator <=(LogLevel a, LogLevel b) {
			return a.value <= b.value;
		}

		public static bool operator <=(LogLevel a, int value) {
			return a.value <= value;
		}

		public static bool operator ==(LogLevel a, LogLevel b) {
			return a.Equals(b);
		}

		public static bool operator ==(LogLevel a, int value) {
			return a.value == value;
		}

		public static bool operator !=(LogLevel a, LogLevel b) {
			return !(a == b);
		}

		public static bool operator !=(LogLevel a, int value) {
			return !(a == value);
		}
	}
}