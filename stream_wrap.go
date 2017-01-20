package rpc

import (
	"bufio"

	inet "github.com/libp2p/go-libp2p-net"
	multicodec "github.com/multiformats/go-multicodec"
	cbor "github.com/multiformats/go-multicodec/cbor"
)

// streamWrap wraps a libp2p stream. We encode/decode whenever we
// write/read from a stream, so we can just carry the encoders
// and bufios with us
type streamWrap struct {
	stream inet.Stream
	enc    multicodec.Encoder
	dec    multicodec.Decoder
	w      *bufio.Writer
	r      *bufio.Reader
}

// wrapStream takes a stream and complements it with r/w bufios and
// decoder/encoder. In order to write to the stream we can use
// wrap.w.Write(). To encode something into it we can wrap.enc.Encode().
// Finally, we should wrap.w.Flush() to actually send the data. Similar
// for receiving.
func wrapStream(s inet.Stream) *streamWrap {
	reader := bufio.NewReader(s)
	writer := bufio.NewWriter(s)
	dec := cbor.Multicodec().Decoder(reader)
	enc := cbor.Multicodec().Encoder(writer)
	return &streamWrap{
		stream: s,
		r:      reader,
		w:      writer,
		enc:    enc,
		dec:    dec,
	}
}
