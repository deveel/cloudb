 using System;
 using System.IO;
 
 using Deveel.Data.Util;
 
namespace Deveel.Data.Store {
 	public class AreaInputStream : Stream, IInputStream {
 		private readonly IArea area;
 		private byte[] buffer;
 		private int count;
 		private int pos;
 		
 		public AreaInputStream(IArea area, int buffer_size) {
 			if (buffer_size <= 0)
 				throw new ArgumentOutOfRangeException("buffer_size", "The buffer size cannot be smaller or equal to 0.");
 			
 			this.area = area;
 			this.buffer = new byte[buffer_size];
 			this.count = 0;
 			this.pos = 0;
 		}
 		
 		public AreaInputStream(IArea area)
 			: this(area, 512) {
 		}
  
public override bool CanRead {
	get { return true; }
}
  
public override bool CanWrite {
	get { return false; }
}
  
  // TODO
public override bool CanSeek {
	get { return false; }
}
  
public override long Length {
	get { return area.Capacity; }
}
  
public override long Position {
	get { return pos; }
	set {
		//TODO
		throw new NotImplementedException();
	}
} 

  /**
   * Reads data from the area into the buffer.  Returns false when end is
   * reached.
   */
  private bool fillBuffer() {
	if (count - pos <= 0) {
	  int read_from_area =
			   Math.Min(area.Capacity - area.Position, buffer.Length);
	  // If can't read any more then return false
	  if (read_from_area == 0) {
		return false;
	  }
	  area.Read(buffer, 0, read_from_area);
	  pos = 0;
	  count = read_from_area;
	}
	return true;
  }

  /**
   * Copies a section from the buffer into the given array and returns the
   * amount actually read.
   */
  private int readFromBuffer(byte[] b, int off, int len) {
	// If we can't fill the buffer, return -1
	if (!fillBuffer()) {
	  return -1;
	}

	// What we can read,
	int to_read = Math.Min(count - pos, len);
	// Read the data,
	Array.Copy(buffer, pos, b, off, to_read);
	// Advance the position
	pos += to_read;
	// Return the amount read,
	return to_read;
  }

  // ----- Implemented from InputStream -----

  public override int ReadByte() {
	// If we can't fill the buffer, return -1
	if (!fillBuffer()) {
	  return -1;
	}

	int p = ((int) buffer[pos]) & 0x0FF;
	++pos;
	return p;
  }

  public override int Read(byte[] b, int off, int len) {
	int has_read = 0;
	// Try and read
	while (len > 0) {
	  int read = readFromBuffer(b, off, len);

	  // If the end of the stream reached
	  if (read == -1) {
		// And something has been read, return the amount we read,
		if (has_read > 0) {
		  return has_read;
		}
		// Otherwise return 0
		else {
		  return 0;
		}
	  }

	  off += read;
	  has_read += read;
	  len -= read;
	}

	return has_read;
  }

  public long Skip(long n) {
	// Make sure n isn't larger than an integer max value
	n = Math.Min(n, Int32.MaxValue);

	if (n > 0) {
	  // Trivially change the area pointer
	  area.Position = area.Position + (int) n;
	  // And empty the buffer
	  pos = 0;
	  count = 0;

	  return n;
	}
	else if (n < 0) {
	  throw new ApplicationException("Negative skip");
	}

	return n;
  }

  public int Available {
  	get { return (area.Capacity - area.Position) + (count - pos); }
  }
  
public override void Write(byte[] buffer, int offset, int count)
{
	throw new NotSupportedException();
}

public override void SetLength(long value)
{
	throw new NotSupportedException();
}

public override long Seek(long offset, SeekOrigin origin)
{
	throw new NotImplementedException();
}
  
public override void Flush()
{
}

  public override void Close() {
	// Nothing to do here,
  }

	
	bool IInputStream.MarkSupported {
		get { return false; }
	}
	
	void IInputStream.Mark(int readLimit) {
	}
	
	void IInputStream.Reset() {
	}
}

}