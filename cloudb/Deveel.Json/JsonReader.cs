using System;
using System.IO;
using System.Text;

namespace Deveel.Json {
	internal class JSONReader {
		private readonly TextReader reader;
		private int character;
		private bool eof;
		private int index;
		private int line;
		private char previous;
		private bool usePrevious;

		public JSONReader(TextReader reader) {
			this.reader = reader;
			eof = false;
			usePrevious = false;
			previous = '\0';
			index = 0;
			character = 1;
			line = 1;
		}

		public JSONReader(Stream stream)
			: this(new StreamReader(new BufferedStream(stream))) {
		}

		public JSONReader(string s)
			: this(new StringReader(s)) {
		}

		public void ReadBack() {
			if (usePrevious || index <= 0)
				throw new JSONException("Stepping back two steps is not supported");

			index -= 1;
			character -= 1;
			usePrevious = true;
			eof = false;
		}

		public bool IsEOF {
			get { return eof && !usePrevious; }
		}

		public bool Read() {
			ReadChar();
			if (IsEOF)
				return false;
			ReadBack();
			return true;
		}

		public char ReadChar() {
			int c;
			if (usePrevious) {
				usePrevious = false;
				c = previous;
			} else {
				try {
					c = reader.Read();
				} catch(IOException exception) {
					throw new JSONException(exception.Message, exception);
				}

				if (c <= 0) {
					// End of stream
					eof = true;
					c = 0;
				}
			}
			index += 1;
			if (previous == '\r') {
				line += 1;
				character = c == '\n' ? 0 : 1;
			} else if (c == '\n') {
				line += 1;
				character = 0;
			} else {
				character += 1;
			}
			previous = (char) c;
			return previous;
		}

		public char[] ReadChars(int n) {
			if (n == 0)
				return new char[0];

			char[] buffer = new char[n];
			int pos = 0;

			while (pos < n) {
				buffer[pos] = ReadChar();
				if (IsEOF) {
					throw SyntaxError("Substring bounds error");
				}
				pos += 1;
			}
			return (char[]) buffer.Clone();
		}

		public char ReadCharClean() {
			while (true) {
				char c = ReadChar();
				if (c == 0 || c > ' ') {
					return c;
				}
			}
		}

		public String ReadString(char quote) {
			char c;
			StringBuilder sb = new StringBuilder();
			for (;;) {
				c = ReadChar();
				switch(c) {
					case '\0':
					case '\n':
					case '\r':
						throw SyntaxError("Unterminated string");
					case '\\':
						c = ReadChar();
						switch(c) {
							case 'b':
								sb.Append('\b');
								break;
							case 't':
								sb.Append('\t');
								break;
							case 'n':
								sb.Append('\n');
								break;
							case 'f':
								sb.Append('\f');
								break;
							case 'r':
								sb.Append('\r');
								break;
							case 'u':
								sb.Append((char) Convert.ToInt32(new string(ReadChars(4)), 16));
								break;
							case '"':
							case '\'':
							case '\\':
							case '/':
								sb.Append(c);
								break;
							default:
								throw SyntaxError("Illegal escape.");
						}
						break;
					default:
						if (c == quote) {
							return sb.ToString();
						}
						sb.Append(c);
						break;
				}
			}
		}

		public String ReadTo(char d) {
			StringBuilder sb = new StringBuilder();
			for (;;) {
				char c = ReadChar();
				if (c == d || c == 0 || c == '\n' || c == '\r') {
					if (c != 0) {
						ReadBack();
					}
					return sb.ToString().Trim();
				}
				sb.Append(c);
			}
		}

		public String ReadTo(String delimiters) {
			char c;
			StringBuilder sb = new StringBuilder();
			for (;;) {
				c = ReadChar();
				if (delimiters.IndexOf(c) >= 0 || c == 0 ||
				    c == '\n' || c == '\r') {
					if (c != 0) {
						ReadBack();
					}
					return sb.ToString().Trim();
				}
				sb.Append(c);
			}
		}

		public object ReadValue() {
			char c = ReadCharClean();
			String s;

			switch(c) {
				case '"':
				case '\'':
					return ReadString(c);
				case '{':
					ReadBack();
					return new JSONObject(this);
				case '[':
				case '(':
					ReadBack();
					return new JSONArray(this);
			}

			StringBuilder sb = new StringBuilder();
			while (c >= ' ' && ",:]}/\\\"[{;=#".IndexOf(c) < 0) {
				sb.Append(c);
				c = ReadChar();
			}
			ReadBack();

			s = sb.ToString().Trim();
			if (s.Equals("")) {
				throw SyntaxError("Missing value");
			}
			return JSONObject.StringToValue(s);
		}

		public char SkipTo(char to) {
			char c;
			try {
				int startIndex = index;
				int startCharacter = character;
				int startLine = line;
				do {
					c = ReadChar();
					if (c == 0) {
						index = startIndex;
						character = startCharacter;
						line = startLine;
						return c;
					}
				} while (c != to);
			} catch(IOException exc) {
				throw new JSONException(exc.Message, exc);
			}

			ReadBack();
			return c;
		}

		internal JSONException SyntaxError(string message) {
			return new JSONSyntaxException(message, index, character, line);
		}
	}
}