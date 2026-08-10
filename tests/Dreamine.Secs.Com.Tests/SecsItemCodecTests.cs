using Dreamine.Secs.Abstractions.Model;
using Dreamine.Secs.Abstractions.Validation;
using Dreamine.Secs.Com.Codecs;
using Xunit;

namespace Dreamine.Secs.Com.Tests;

public sealed class SecsItemCodecTests
{
    private readonly SecsItemCodec _codec = new();

    public static TheoryData<SecsItem> SupportedItems => new()
    {
        new SecsListItem(new SecsBinaryItem(0, 255), new SecsListItem(new SecsAsciiItem("ABC"))),
        new SecsListItem(),
        new SecsBinaryItem(), new SecsBinaryItem(0, 1, 255),
        new SecsBooleanItem(), new SecsBooleanItem(false, true, true),
        new SecsAsciiItem(string.Empty), new SecsAsciiItem("ABC"),
        new SecsJis8Item(), new SecsJis8Item(0x21, 0xa1),
        new SecsInt8Item(sbyte.MinValue, -1, 0, sbyte.MaxValue),
        new SecsInt16Item(short.MinValue, -1, 0, short.MaxValue),
        new SecsInt32Item(int.MinValue, -1, 0, int.MaxValue),
        new SecsInt64Item(long.MinValue, -1, 0, long.MaxValue),
        new SecsUInt8Item(byte.MinValue, byte.MaxValue),
        new SecsUInt16Item(ushort.MinValue, ushort.MaxValue),
        new SecsUInt32Item(uint.MinValue, uint.MaxValue),
        new SecsUInt64Item(ulong.MinValue, ulong.MaxValue),
        new SecsFloat32Item(float.NegativeInfinity, -1.25f, 0, float.PositiveInfinity, float.NaN),
        new SecsFloat64Item(double.NegativeInfinity, -1.25, 0, double.PositiveInfinity, double.NaN)
    };

    [Theory]
    [MemberData(nameof(SupportedItems))]
    public void EverySupportedFormatRoundTrips(SecsItem item)
    {
        var decoded = _codec.Decode(_codec.Encode(item));
        AssertEquivalent(item, decoded);
    }

    [Fact]
    public void GoldenBytesMatchE5Examples()
    {
        Assert.Equal(new byte[] { 0x21, 0x01, 0xaa }, _codec.Encode(new SecsBinaryItem(0xaa)));
        Assert.Equal(new byte[] { 0x41, 0x03, 0x41, 0x42, 0x43 }, _codec.Encode(new SecsAsciiItem("ABC")));
        Assert.Equal(new byte[] { 0x69, 0x06, 0x00, 0x01, 0xff, 0xff, 0x7f, 0xff }, _codec.Encode(new SecsInt16Item(1, -1, short.MaxValue)));
        Assert.Equal(new byte[] { 0x91, 0x04, 0x3f, 0x80, 0x00, 0x00 }, _codec.Encode(new SecsFloat32Item(1f)));
    }

    [Fact]
    public void LengthHeaderUsesOneTwoAndThreeBytes()
    {
        foreach (var (length, lengthBytes) in new[] { (0, 1), (255, 1), (256, 2), (65_535, 2), (65_536, 3) })
        {
            var encoded = _codec.Encode(new SecsBinaryItem(new byte[length]));
            Assert.Equal(lengthBytes, encoded[0] & 3);
            Assert.Equal(length, Assert.IsType<SecsBinaryItem>(_codec.Decode(encoded)).Values.Length);
        }
    }

    [Theory]
    [InlineData(new byte[] { 0x22 })]
    [InlineData(new byte[] { 0x22, 0x00 })]
    [InlineData(new byte[] { 0x23 })]
    [InlineData(new byte[] { 0x23, 0x00 })]
    [InlineData(new byte[] { 0x23, 0x00, 0x00 })]
    public void TruncatedLengthFieldIsRejected(byte[] bytes)
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(bytes));
        Assert.Equal(SecsValidationCode.Truncated, exception.Code);
    }

    [Theory]
    [InlineData(new byte[] { })]
    [InlineData(new byte[] { 0x21 })]
    [InlineData(new byte[] { 0x21, 0x02, 0x01 })]
    public void TruncatedInputIsRejected(byte[] bytes)
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(bytes));
        Assert.Equal(SecsValidationCode.Truncated, exception.Code);
    }

    [Fact]
    public void ZeroLengthByteCountIsRejected()
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(new byte[] { 0x20 }));
        Assert.Equal(SecsValidationCode.InvalidLength, exception.Code);
    }

    [Fact]
    public void UnknownFormatIsRejected()
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(new byte[] { 0xfd, 0x00 }));
        Assert.Equal(SecsValidationCode.UnsupportedFormat, exception.Code);
    }

    [Fact]
    public void InvalidAtomicWidthIsRejected()
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(new byte[] { 0x69, 0x01, 0x00 }));
        Assert.Equal(SecsValidationCode.InvalidLength, exception.Code);
    }

    [Fact]
    public void TrailingInputIsRejectedAndValidateReturnsStructuredError()
    {
        var result = _codec.Validate(new byte[] { 0x21, 0x00, 0x00 });
        Assert.False(result.IsValid);
        Assert.Equal(SecsValidationCode.TrailingData, result.Code);
        Assert.Equal(2, result.Offset);
    }

    [Fact]
    public void SizeLimitIsAppliedToEncodeAndDecode()
    {
        var codec = new SecsItemCodec(new SecsItemCodecOptions { MaximumMessageLength = 3 });
        Assert.Equal(3, codec.Encode(new SecsBinaryItem(1)).Length);
        Assert.Throws<SecsDecodeException>(() => codec.Encode(new SecsBinaryItem(1, 2)));
        Assert.Throws<SecsDecodeException>(() => codec.Decode(new byte[4]));
    }

    [Fact]
    public void NestingDepthLimitIsAppliedToEncodeAndDecode()
    {
        var codec = new SecsItemCodec(new SecsItemCodecOptions { MaximumNestingDepth = 1 });
        var item = new SecsListItem(new SecsListItem(new SecsListItem()));
        var bytes = _codec.Encode(item);
        Assert.Throws<SecsDecodeException>(() => codec.Encode(item));
        Assert.Throws<SecsDecodeException>(() => codec.Decode(bytes));
    }

    [Fact]
    public void ListBombIsRejectedBeforeAllocatingDeclaredChildren()
    {
        var exception = Assert.Throws<SecsDecodeException>(() => _codec.Decode(new byte[] { 0x03, 0xff, 0xff, 0xff }));
        Assert.Equal(SecsValidationCode.SizeLimitExceeded, exception.Code);

        var bounded = new SecsItemCodec(new SecsItemCodecOptions { MaximumListItemCount = 10 });
        var truncated = Assert.Throws<SecsDecodeException>(() => bounded.Decode(new byte[] { 0x01, 0x02 }));
        Assert.Equal(SecsValidationCode.Truncated, truncated.Code);
    }

    [Fact]
    public void EncodePreflightRejectsOversizedNestedContent()
    {
        var codec = new SecsItemCodec(new SecsItemCodecOptions { MaximumMessageLength = 8 });
        var item = new SecsListItem(new SecsBinaryItem(1, 2, 3), new SecsBinaryItem(4, 5, 6));
        Assert.Equal(SecsValidationCode.SizeLimitExceeded, Assert.Throws<SecsDecodeException>(() => codec.Encode(item)).Code);
    }

    [Fact]
    public void MaximumMessageLengthAcceptsExactBoundaryAndRejectsOneByteMore()
    {
        var codec = new SecsItemCodec(new SecsItemCodecOptions { MaximumMessageLength = 1024 });
        var exact = new SecsBinaryItem(new byte[1021]);

        Assert.Equal(1024, codec.Encode(exact).Length);
        Assert.Equal(1021, Assert.IsType<SecsBinaryItem>(codec.Decode(codec.Encode(exact))).Values.Length);
        Assert.Equal(SecsValidationCode.SizeLimitExceeded, Assert.Throws<SecsDecodeException>(() => codec.Encode(new SecsBinaryItem(new byte[1022]))).Code);
    }

    [Fact]
    public void ArbitraryBytesFailOnlyWithStructuredProtocolErrors()
    {
        const int seed = 20260810;
        var random = new Random(seed);
        var codec = new SecsItemCodec(new SecsItemCodecOptions
        {
            MaximumMessageLength = 1024,
            MaximumNestingDepth = 16,
            MaximumListItemCount = 128
        });

        for (var index = 0; index < 5_000; index++)
        {
            var bytes = RandomBytes(random, random.Next(0, 1025));
            try
            {
                var decoded = codec.Decode(bytes);
                Assert.Equal(bytes, codec.Encode(decoded));
            }
            catch (Exception exception)
            {
                Assert.True(exception is SecsProtocolException, $"Seed {seed}, case {index}, exception {exception.GetType().FullName}");
            }
        }
    }

    [Fact]
    public void RandomValidItemsRoundTripDeterministically()
    {
        var random = new Random(20260809);
        for (var index = 0; index < 500; index++)
        {
            var item = CreateRandomItem(random, 0);
            var encoded = _codec.Encode(item);
            var decoded = _codec.Decode(encoded);
            AssertEquivalent(item, decoded);
            Assert.Equal(encoded, _codec.Encode(decoded));
        }
    }

    private static SecsItem CreateRandomItem(Random random, int depth)
    {
        var kind = random.Next(depth < 3 ? 0 : 1, 15);
        var count = random.Next(0, 8);
        return kind switch
        {
            0 => new SecsListItem(Enumerable.Range(0, random.Next(0, 5)).Select(_ => CreateRandomItem(random, depth + 1)).ToArray()),
            1 => new SecsBinaryItem(RandomBytes(random, count)),
            2 => new SecsBooleanItem(Enumerable.Range(0, count).Select(_ => random.Next(2) == 1).ToArray()),
            3 => new SecsAsciiItem(new string(Enumerable.Range(0, count).Select(_ => (char)random.Next(32, 127)).ToArray())),
            4 => new SecsJis8Item(RandomBytes(random, count)),
            5 => new SecsInt8Item(Enumerable.Range(0, count).Select(_ => (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1)).ToArray()),
            6 => new SecsInt16Item(Enumerable.Range(0, count).Select(_ => (short)random.Next(short.MinValue, short.MaxValue + 1)).ToArray()),
            7 => new SecsInt32Item(Enumerable.Range(0, count).Select(_ => random.Next()).ToArray()),
            8 => new SecsInt64Item(Enumerable.Range(0, count).Select(_ => random.NextInt64()).ToArray()),
            9 => new SecsUInt8Item(RandomBytes(random, count)),
            10 => new SecsUInt16Item(Enumerable.Range(0, count).Select(_ => (ushort)random.Next(ushort.MaxValue + 1)).ToArray()),
            11 => new SecsUInt32Item(Enumerable.Range(0, count).Select(_ => (uint)random.NextInt64(0, (long)uint.MaxValue + 1)).ToArray()),
            12 => new SecsUInt64Item(Enumerable.Range(0, count).Select(_ => (ulong)random.NextInt64()).ToArray()),
            13 => new SecsFloat32Item(Enumerable.Range(0, count).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray()),
            _ => new SecsFloat64Item(Enumerable.Range(0, count).Select(_ => random.NextDouble() * 2 - 1).ToArray())
        };
    }

    private static byte[] RandomBytes(Random random, int count) { var bytes = new byte[count]; random.NextBytes(bytes); return bytes; }

    private static void AssertEquivalent(SecsItem expected, SecsItem actual)
    {
        Assert.Equal(expected.Format, actual.Format);
        Assert.Equal(expected.Count, actual.Count);
        switch (expected)
        {
            case SecsListItem left:
                var right = Assert.IsType<SecsListItem>(actual);
                for (var i = 0; i < left.Count; i++) AssertEquivalent(left.Items[i], right.Items[i]);
                break;
            case SecsAsciiItem left: Assert.Equal(left.Value, Assert.IsType<SecsAsciiItem>(actual).Value); break;
            case SecsBinaryItem left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsBinaryItem>(actual).Values.ToArray()); break;
            case SecsBooleanItem left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsBooleanItem>(actual).Values.ToArray()); break;
            case SecsJis8Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsJis8Item>(actual).Values.ToArray()); break;
            case SecsInt8Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsInt8Item>(actual).Values.ToArray()); break;
            case SecsInt16Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsInt16Item>(actual).Values.ToArray()); break;
            case SecsInt32Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsInt32Item>(actual).Values.ToArray()); break;
            case SecsInt64Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsInt64Item>(actual).Values.ToArray()); break;
            case SecsUInt8Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsUInt8Item>(actual).Values.ToArray()); break;
            case SecsUInt16Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsUInt16Item>(actual).Values.ToArray()); break;
            case SecsUInt32Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsUInt32Item>(actual).Values.ToArray()); break;
            case SecsUInt64Item left: Assert.Equal(left.Values.ToArray(), Assert.IsType<SecsUInt64Item>(actual).Values.ToArray()); break;
            case SecsFloat32Item left: Assert.Equal(left.Values.Span.ToArray().Select(BitConverter.SingleToInt32Bits), Assert.IsType<SecsFloat32Item>(actual).Values.Span.ToArray().Select(BitConverter.SingleToInt32Bits)); break;
            case SecsFloat64Item left: Assert.Equal(left.Values.Span.ToArray().Select(BitConverter.DoubleToInt64Bits), Assert.IsType<SecsFloat64Item>(actual).Values.Span.ToArray().Select(BitConverter.DoubleToInt64Bits)); break;
            default: throw new InvalidOperationException(expected.GetType().FullName);
        }
    }
}
