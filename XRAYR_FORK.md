# XrayR xray-core fork

This branch is based on `github.com/xtls/xray-core` v26.10.21 and is pinned by
XrayR through an exact pseudo-version `replace` directive.

Intentional differences:

- Linux TCP splice is split into 32 MiB `io.LimitedReader` chunks so stats are
  credited periodically instead of only when the connection closes.
- Each splice read also has a controller-selected deadline (30 seconds in
  XrayR), so slow connections credit partial traffic and recheck state without
  waiting to fill a complete chunk.
- A context `proxy.SpliceController` is checked at byte or time checkpoints. It
  may keep splicing, fall back to the buffered path, or reject the connection.
  On fallback the read deadline is cleared and both activity timers return to
  the controller's normal policy idle timeout.
- Linux socket tests cover exact multi-chunk accounting, partial accounting at
  a time checkpoint, buffered fallback, rejection, and timer restoration.

Keep `app/dispatcher.SizeStatWriter` as the concrete writer recognized by the
splice path. Rebase these changes deliberately when updating xray-core.
