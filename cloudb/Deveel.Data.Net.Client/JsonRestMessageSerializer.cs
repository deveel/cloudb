﻿using System;
using System.IO;

namespace Deveel.Data.Net.Client {
	public sealed class JsonRestMessageSerializer : JsonMessageSerializer, IMessageStreamSupport {
		protected override void Deserialize(Message message, TextReader reader) {
			throw new NotImplementedException();
		}
		
		protected override void Serialize(Message message, TextWriter writer) {
			throw new NotImplementedException();
		}
	}
}