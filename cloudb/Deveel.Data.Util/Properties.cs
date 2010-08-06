// Derived from Apache Harmony java.util.Properties class

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Util {
	public class Properties : Dictionary<object, object> {
		public Properties()
			: base() {
		}

		public Properties(Properties defaults) {
			this.defaults = defaults;
		}

		private const int NONE = 0,
						  SLASH = 1,
						  UNICODE = 2,
						  CONTINUE = 3,
						  KEY_DONE = 4,
						  IGNORE = 5;

		protected Properties defaults;

		private static string lineSeparator;

		public ICollection<string> PropertyNames {
			get {
				Dictionary<string, object> selected = new Dictionary<string, object>();
				SelectProperties(selected);
				return selected.Keys;
			}
		}

		private void SelectProperties(IDictionary<string, object> selectProperties) {
			if (defaults != null) {
				defaults.SelectProperties(selectProperties);
			}

			ICollection<object> keys = Keys;
			foreach (object key in keys) {
				// Only select property with string key and value
				if (key is String) {
					object value = base[key];
					if (value is string) {
						selectProperties.Add((string)key, value);
					}
				}
			}
		}

		private void dumpString(StringBuilder buffer, String s, bool key) {
			int i = 0;
			if (!key && i < s.Length && s[i] == ' ') {
				buffer.Append("\\ "); //$NON-NLS-1$
				i++;
			}

			for (; i < s.Length; i++) {
				char ch = s[i];
				switch (ch) {
					case '\t':
						buffer.Append("\\t"); //$NON-NLS-1$
						break;
					case '\n':
						buffer.Append("\\n"); //$NON-NLS-1$
						break;
					case '\f':
						buffer.Append("\\f"); //$NON-NLS-1$
						break;
					case '\r':
						buffer.Append("\\r"); //$NON-NLS-1$
						break;
					default:
						if ("\\#!=:".IndexOf(ch) >= 0 || (key && ch == ' ')) {
							buffer.Append('\\');
						}
						if (ch >= ' ' && ch <= '~') {
							buffer.Append(ch);
						} else {
							string hex = ((int)ch).ToString("0x{0:x}");
							buffer.Append("\\u"); //$NON-NLS-1$
							for (int j = 0; j < 4 - hex.Length; j++) {
								buffer.Append("0"); //$NON-NLS-1$
							}
							buffer.Append(hex);
						}
						break;
				}
			}
		}

		public string GetProperty(string name) {
			object result;
			if (!TryGetValue(name, out result))
				return null;

			string property = result is string ? (string)result : null;
			if (property == null && defaults != null)
				property = defaults.GetProperty(name);
			return property;
		}

		public string GetProperty(string name, string defaultValue) {
			object result;
			if (!TryGetValue(name, out result))
				return defaultValue;

			string property = result is string ? (string)result : null;
			if (property == null && defaults != null)
				property = defaults.GetProperty(name);
			if (property == null)
				return defaultValue;
			return property;
		}

		public object SetProperty(string name, string value) {
			return base[name] = value;
		}

		public void Load(Stream input) {
			lock (this) {
				if (input == null)
					throw new ArgumentNullException("input");
				if (!input.CanRead)
					throw new ArgumentException();

				BufferedStream bis = new BufferedStream(input);
				bool isEbcdic;

				try {
					long pos = bis.Position;
					isEbcdic = IsEbcdic(bis);
					bis.Seek(pos, SeekOrigin.Begin);
				} catch {
					isEbcdic = false;
				}

				if (!isEbcdic) {
					Load(new StreamReader(bis, Encoding.GetEncoding("ISO8859-1"))); //$NON-NLS-1$
				} else {
					Load(new StreamReader(bis)); //$NON-NLS-1$
				}
			}
		}

		private static bool IsEbcdic(Stream input) {
			byte b;
			while ((b = (byte)input.ReadByte()) != -1) {
				if (b == 0x23 || b == 0x0a || b == 0x3d) {//ascii: newline/#/=
					return false;
				}
				if (b == 0x15) {//EBCDIC newline
					return true;
				}
			}
			//we found no ascii newline, '#', neither '=', relative safe to consider it
			//as non-ascii, the only exception will be a single line with only key(no value and '=')
			//in this case, it should be no harm to read it in default charset
			return false;
		}

		public void Load(TextReader reader) {
			lock (this) {
				int mode = NONE, unicode = 0, count = 0;
				char nextChar;
				char[] buf = new char[40];
				int offset = 0, keyLength = -1, intVal;
				bool firstChar = true;

				while (true) {
					intVal = reader.Read();
					if (intVal == -1) break;
					nextChar = (char)intVal;

					if (offset == buf.Length) {
						char[] newBuf = new char[buf.Length * 2];
						Array.Copy(buf, 0, newBuf, 0, offset);
						buf = newBuf;
					}
					if (mode == UNICODE) {
						int digit = Convert.ToInt32(nextChar.ToString(), 16);
						if (digit >= 0) {
							unicode = (unicode << 4) + digit;
							if (++count < 4) {
								continue;
							}
						} else if (count <= 4) {
							// luni.09=Invalid Unicode sequence: illegal character
							throw new ArgumentException();
						}
						mode = NONE;
						buf[offset++] = (char)unicode;
						if (nextChar != '\n' && nextChar != '\u0085') {
							continue;
						}
					}
					if (mode == SLASH) {
						mode = NONE;
						switch (nextChar) {
							case '\r':
								mode = CONTINUE; // Look for a following \n
								continue;
							case '\u0085':
							case '\n':
								mode = IGNORE; // Ignore whitespace on the next line
								continue;
							case 'b':
								nextChar = '\b';
								break;
							case 'f':
								nextChar = '\f';
								break;
							case 'n':
								nextChar = '\n';
								break;
							case 'r':
								nextChar = '\r';
								break;
							case 't':
								nextChar = '\t';
								break;
							case 'u':
								mode = UNICODE;
								unicode = count = 0;
								continue;
						}
					} else {
						switch (nextChar) {
							case '#':
							case '!':
								if (firstChar) {
									while (true) {
										intVal = reader.Read();
										if (intVal == -1) break;
										nextChar = (char)intVal; // & 0xff
										// not
										// required
										if (nextChar == '\r' || nextChar == '\n' || nextChar == '\u0085') {
											break;
										}
									}
									continue;
								}
								break;
							case '\n':
								if (mode == CONTINUE) { // Part of a \r\n sequence
									mode = IGNORE; // Ignore whitespace on the next line
									continue;
								}
								// fall into the next case
								goto case '\r';
							case '\u0085':
							case '\r':
								mode = NONE;
								firstChar = true;
								if (offset > 0 || (offset == 0 && keyLength == 0)) {
									if (keyLength == -1) {
										keyLength = offset;
									}
									String temp = new String(buf, 0, offset);
									Add(temp.Substring(0, keyLength), temp.Substring(keyLength));
								}
								keyLength = -1;
								offset = 0;
								continue;
							case '\\':
								if (mode == KEY_DONE) {
									keyLength = offset;
								}
								mode = SLASH;
								continue;
							case ':':
							case '=':
								if (keyLength == -1) { // if parsing the key
									mode = NONE;
									keyLength = offset;
									continue;
								}
								break;
						}
						if (Char.IsWhiteSpace(nextChar)) {
							if (mode == CONTINUE) {
								mode = IGNORE;
							}
							// if key length == 0 or value length == 0
							if (offset == 0 || offset == keyLength || mode == IGNORE) {
								continue;
							}
							if (keyLength == -1) { // if parsing the key
								mode = KEY_DONE;
								continue;
							}
						}
						if (mode == IGNORE || mode == CONTINUE) {
							mode = NONE;
						}
					}
					firstChar = false;
					if (mode == KEY_DONE) {
						keyLength = offset;
						mode = NONE;
					}
					buf[offset++] = nextChar;
				}
				if (mode == UNICODE && count <= 4) {
					// luni.08=Invalid Unicode sequence: expected format \\uxxxx
					throw new ArgumentException();
				}
				if (keyLength == -1 && offset > 0) {
					keyLength = offset;
				}
				if (keyLength >= 0) {
					String temp = new String(buf, 0, offset);
					String key = temp.Substring(0, keyLength);
					String value = temp.Substring(keyLength);
					if (mode == SLASH) {
						value += "\u0000";
					}
					Add(key, value);
				}
			}
		}

		public void Store(Stream output, String comment) {
			lock (this) {
				if (lineSeparator == null) {
					lineSeparator = Environment.NewLine; //$NON-NLS-1$
				}

				StringBuilder buffer = new StringBuilder(200);
				StreamWriter writer = new StreamWriter(output, Encoding.GetEncoding("ISO-8859-1")); //$NON-NLS-1$
				if (comment != null) {
					writer.Write("#"); //$NON-NLS-1$
					writer.Write(comment);
					writer.Write(lineSeparator);
				}
				writer.Write("#"); //$NON-NLS-1$
				writer.Write(DateTime.Now.ToString());
				writer.Write(lineSeparator);

				foreach (KeyValuePair<object, object> pair in this) {
					string key = (string)pair.Key;
					dumpString(buffer, key, true);
					buffer.Append('=');
					dumpString(buffer, (String)pair.Value, false);
					buffer.Append(lineSeparator);
					writer.Write(buffer.ToString());
					buffer.Length = 0;
				}
				writer.Flush();
			}
		}

		public void Store(TextWriter writer, String comment) {
			lock (this) {
				if (lineSeparator == null) {
					lineSeparator = Environment.NewLine; //$NON-NLS-1$
				}
				StringBuilder buffer = new StringBuilder(200);
				if (comment != null) {
					writer.Write("#"); //$NON-NLS-1$
					writer.Write(comment);
					writer.Write(lineSeparator);
				}
				writer.Write("#"); //$NON-NLS-1$
				writer.Write(DateTime.Now.ToString());
				writer.Write(lineSeparator);

				foreach (KeyValuePair<object, object> pair in this) {
					String key = (String)pair.Key;
					dumpString(buffer, key, true);
					buffer.Append('=');
					dumpString(buffer, (String)pair.Value, false);
					buffer.Append(lineSeparator);
					writer.Write(buffer.ToString());
					buffer.Length = 0;
				}
				writer.Flush();
			}
		}
	}
}