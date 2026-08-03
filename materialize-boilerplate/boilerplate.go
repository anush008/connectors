package boilerplate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	pm "github.com/estuary/flow/go/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// DestinationReader enumerates the documents a materialized resource currently
// holds. It is optional: a connector which does not implement it simply cannot be
// verified by a harness that reads destinations, and is otherwise unaffected.
//
// This exists so that an exactly-once test harness can check what actually landed,
// rather than trusting the connector's own account of what it wrote. Verification
// that asks the subject under test to report on itself proves very little, and the
// protocol offers no way to read a destination back — Load answers only the keys the
// runtime asks about, and only for bindings that are not delta-updates.
//
// `out` is called once per document, in whatever order the destination returns them;
// a harness that cares about order must sort. Implementations should stream rather
// than accumulate: a materialized table can be far larger than memory.
type DestinationReader interface {
	ReadDestination(
		ctx context.Context,
		endpointConfig json.RawMessage,
		resourceConfig json.RawMessage,
		out func(json.RawMessage) error,
	) error
}

type Connector interface {
	Spec(context.Context, *pm.Request_Spec) (*pm.Response_Spec, error)
	Validate(context.Context, *pm.Request_Validate) (*pm.Response_Validated, error)
	Apply(context.Context, *pm.Request_Apply) (*pm.Response_Applied, error)
	NewTransactor(context.Context, pm.Request_Open, *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error)
}

// RunMain is the boilerplate main function of a materialization connector.
//
// With no arguments it serves the materialization protocol on stdin/stdout, which is
// how the runtime invokes a connector. The `read` subcommand is the one exception,
// and exists for test harnesses: see runRead.
func RunMain(connector Connector) {
	if len(os.Args) > 1 && os.Args[1] == "read" {
		runRead(connector, os.Args[2:])
		return
	}

	switch format := getEnvDefault("LOG_FORMAT", "color"); format {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	case "text":
		log.SetFormatter(&log.TextFormatter{})
	case "color":
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	default:
		log.WithField("format", format).Fatal("invalid LOG_FORMAT (expected 'json', 'text', or 'color')")
	}

	lvl, err := log.ParseLevel(getEnvDefault("LOG_LEVEL", "info"))
	if err != nil {
		log.WithFields(log.Fields{"level": lvl, "error": err}).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}

	common.ConfigureMemoryLimit()

	var ctx, _ = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	var stream m.Stream

	switch codec := getEnvDefault("FLOW_RUNTIME_CODEC", "proto"); codec {
	case "proto":
		log.Debug("using protobuf codec")
		stream = newProtoCodec()
	case "json":
		log.Debug("using json codec")
		stream = newJsonCodec()
	default:
		log.WithField("codec", codec).Fatal("invalid FLOW_RUNTIME_CODEC (expected 'json', or 'proto')")
	}

	go func() {
		log.WithField("port", 6060).Debug("starting pprof server")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.WithField("err", err).Info("pprof server shut down unexpectedly")
		}
	}()

	if err := materialize(ctx, stream, connector, lvl); err != nil {
		cerrors.HandleFinalError(err)
	}
	os.Exit(0)
}

func getEnvDefault(name, def string) string {
	var s = os.Getenv(name)
	if s == "" {
		return def
	}
	return s
}

func materialize(ctx context.Context, stream m.Stream, connector Connector, lvl log.Level) error {
	for {
		var request pm.Request
		if err := stream.RecvMsg(&request); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err = request.Validate_(); err != nil {
			return fmt.Errorf("validating request: %w", err)
		}

		switch {
		case request.Spec != nil:
			var response, err = connector.Spec(ctx, request.Spec)
			if err != nil {
				return err
			}
			response.Protocol = 3032023

			if err := stream.Send(&pm.Response{Spec: response}); err != nil {
				return err
			}
		case request.Validate != nil:
			if response, err := connector.Validate(ctx, request.Validate); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Validated: response}); err != nil {
				return err
			}
		case request.Apply != nil:
			if response, err := connector.Apply(ctx, request.Apply); err != nil {
				return err
			} else if err := stream.Send(&pm.Response{Applied: response}); err != nil {
				return err
			}
		case request.Open != nil:
			return m.RunTransactions(ctx, connector, stream, request.Open, lvl)
		default:
			return fmt.Errorf("unexpected request %#v", request)
		}
	}
}

func newProtoCodec() m.Stream {
	return &protoCodec{
		r: bufio.NewReaderSize(os.Stdin, 1<<21),
		w: protoio.NewUint32DelimitedWriter(os.Stdout, binary.LittleEndian),
	}
}

type protoCodec struct {
	r *bufio.Reader
	w protoio.Writer
}

func (c *protoCodec) Send(m *pm.Response) error {
	return c.w.WriteMsg(m)
}

func (c *protoCodec) RecvMsg(m *pm.Request) error {
	var lengthBytes [4]byte

	if _, err := io.ReadFull(c.r, lengthBytes[:]); err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("reading message length: %w", err)
	}

	var len = int(binary.LittleEndian.Uint32(lengthBytes[:]))

	var b, err = c.peekMessage(len)
	if err != nil {
		return err
	}

	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}
	return nil
}

func (c *protoCodec) peekMessage(size int) ([]byte, error) {
	// TODO(johnny): Remove me when we resolve the json.RawMessage casting issue.
	var buf = make([]byte, size)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return nil, fmt.Errorf("reading message (directly): %w", err)
	}
	return buf, nil

	// Fetch next length-delimited message into a buffer.
	// In the garden-path case, we decode directly from the
	// bufio.Reader internal buffer without copying
	// (having just read the length, the message itself
	// is probably _already_ in the bufio.Reader buffer).
	//
	// If the message is larger than the internal buffer,
	// we allocate and read it directly.
	var bs, err = c.r.Peek(size)
	if err == nil {
		// The Discard contract guarantees we won't error.
		// It's safe to reference |bs| until the next Peek.
		if _, err = c.r.Discard(size); err != nil {
			panic(err)
		}
		return bs, nil
	}

	// Non-garden path: we must allocate and read a larger buffer.
	if errors.Is(err, bufio.ErrBufferFull) {
		bs = make([]byte, size)
		if _, err = io.ReadFull(c.r, bs); err != nil {
			return nil, fmt.Errorf("reading message (directly): %w", err)
		}
		return bs, nil
	}

	return nil, fmt.Errorf("reading message (into buffer): %w", err)
}

func newJsonCodec() m.Stream {
	return &jsonCodec{
		marshaler: jsonpb.Marshaler{
			EnumsAsInts:  false,
			EmitDefaults: false,
			Indent:       "", // Compact.
			OrigName:     false,
			AnyResolver:  nil,
		},
		unmarshaler: jsonpb.Unmarshaler{
			AllowUnknownFields: true,
			AnyResolver:        nil,
		},
		decoder: json.NewDecoder(bufio.NewReaderSize(os.Stdin, 1<<21)),
	}
}

type jsonCodec struct {
	marshaler   jsonpb.Marshaler
	unmarshaler jsonpb.Unmarshaler
	decoder     *json.Decoder
}

func (c *jsonCodec) Send(m *pm.Response) error {
	var w bytes.Buffer

	if err := c.marshaler.Marshal(&w, m); err != nil {
		return fmt.Errorf("marshal response to json: %w", err)
	}
	_ = w.WriteByte('\n')

	var _, err = os.Stdout.Write(w.Bytes())
	return err
}

func (c *jsonCodec) RecvMsg(m *pm.Request) error {
	m.Reset()
	return c.unmarshaler.UnmarshalNext(c.decoder, m)
}

// runRead prints the documents of one materialized resource as newline-delimited
// JSON, for a harness verifying what a connector actually wrote.
//
// Deliberately a subcommand rather than a protocol message. The materialization
// protocol has no "read your destination back" request and should not grow one for
// the benefit of tests: the runtime would never send it, so it would be dead weight
// in every connector and a second code path to keep honest. A subcommand is invoked
// only by whoever wants it.
func runRead(connector Connector, args []string) {
	var flags = flag.NewFlagSet("read", flag.ExitOnError)
	var configPath = flags.String("config", "", "path to the endpoint configuration, as JSON or YAML")
	var resourcePath = flags.String("resource", "", "path to the resource configuration, as JSON or YAML")
	if err := flags.Parse(args); err != nil {
		log.WithField("error", err).Fatal("parsing read arguments")
	}

	reader, ok := connector.(DestinationReader)
	if !ok {
		log.Fatal("this connector cannot read its destination: it does not implement DestinationReader")
	}

	var endpointConfig, resourceConfig json.RawMessage
	for _, arg := range []struct {
		path string
		name string
		into *json.RawMessage
	}{
		{*configPath, "config", &endpointConfig},
		{*resourcePath, "resource", &resourceConfig},
	} {
		if arg.path == "" {
			log.Fatalf("--%s is required", arg.name)
		}
		raw, err := os.ReadFile(arg.path)
		if err != nil {
			log.WithField("error", err).Fatalf("reading --%s", arg.name)
		}
		// Accepted as YAML, which is how these files are written in the connectors
		// repository, and which subsumes JSON.
		var intermediate any
		if err := yaml.Unmarshal(raw, &intermediate); err != nil {
			log.WithField("error", err).Fatalf("parsing --%s", arg.name)
		}
		parsed, err := json.Marshal(intermediate)
		if err != nil {
			log.WithField("error", err).Fatalf("converting --%s to JSON", arg.name)
		}
		*arg.into = parsed
	}

	var ctx, cancel = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	var out = bufio.NewWriter(os.Stdout)
	if err := reader.ReadDestination(ctx, endpointConfig, resourceConfig, func(doc json.RawMessage) error {
		if _, err := out.Write(doc); err != nil {
			return err
		}
		_, err := out.WriteString("\n")
		return err
	}); err != nil {
		log.WithField("error", err).Fatal("reading the destination")
	}
	if err := out.Flush(); err != nil {
		log.WithField("error", err).Fatal("flushing the destination read")
	}
}
