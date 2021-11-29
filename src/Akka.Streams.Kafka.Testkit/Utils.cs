using System;
using System.IO;
using System.Linq;

namespace Akka.Streams.Kafka.Testkit
{
    public static class BinaryUtils
    {
        public static short ReadInt16BE(this BinaryReader reader)
        {
            var bytes = reader.ReadBytes(2);
            if (BitConverter.IsLittleEndian)
                bytes = bytes.Reverse().ToArray();
            return BitConverter.ToInt16(bytes, 0);
        }
        
        public static short ReadInt32BE(this BinaryReader reader)
        {
            var bytes = reader.ReadBytes(4);
            if (BitConverter.IsLittleEndian)
                bytes = bytes.Reverse().ToArray();
            return BitConverter.ToInt16(bytes, 0);
        }
    }
}