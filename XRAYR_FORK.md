# XrayR xray-core fork

This directory is pinned from `github.com/xtls/xray-core` at the version in the
root `go.mod` and is selected by a local `replace` directive.

Intentional differences:

- Linux TCP splice is split into 32 MiB `io.LimitedReader` chunks so stats are
  credited periodically instead of only when the connection closes.
- An optional context `proxy.SpliceController` is checked between chunks. It
  may keep splicing, fall back to the buffered path, or reject the connection.

Keep `app/dispatcher.SizeStatWriter` as the concrete writer recognized by the
splice path. Rebase these changes deliberately when updating xray-core.
