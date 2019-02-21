# tokio-capnp

Experimenting with building capnp message transport
With following considerations in mind:
+ Avoid messages splitting (https://github.com/capnproto/capnproto-rust/issues/93)
+ If possible, achive prevoius goal without copying.
+ capnp-futures implementation of reads and writes is reactor unaware, but it should be. Is it
though? #TODO: prove or disprove.

First problem is ideomaticlly fixed by wrapping stream inside WriteBuf.
But it brings need to manage flushing, and it is not copy free.

Question: is it ok to flush after every message send?

Other solution is to allocate first segment with some extra space before
actual segment. Then, if it large enough to fit segment table
write segment table to it's end, and then concatenate slices
via little of unsafe.

Third problem requires implementing it over AsyncWrite and AsyncRead, which either
mean depending on tokio, or adding such behaviour after futures stabilization.
Latter is preferable IMO.
