# Lightpool security contract

Ordinary execution supports shared-key AES-GCM over both datagrams and
stream frames. Supply `Config.Keyring`, constructed with `NewKeyring`.
Keys are 32 bytes (AES-256). Every member holding a key is trusted;
this provides neither individual peer authentication nor Byzantine fault
tolerance. Without a keyring, frames are plaintext; CRC32 detects corruption
but does not authenticate senders.

The primary key encrypts. All installed keys can decrypt. Rotate by adding
the new key everywhere (`Add`), selecting it (`Use`), then removing the old
key everywhere (`Remove`). The primary key cannot be removed. Key buffers
are copied at the API boundary. Encryption cannot be disabled through a
migration flag; encrypted nodes reject plaintext.

`Protocol.Label` is public but authenticated by AES-GCM. A different label
is rejected. Headers, lengths and flags are also authenticated. Nonces come
from the standard library's secure runtime entropy, independently of seeded
protocol randomness. The simulator rejects encryption configuration.

Every frame is sealed under the primary key with a fresh random 96-bit
nonce. Random nonces bound one key to about 2^32 sealed frames; past that, a
repeated nonce becomes likely enough to matter, and a repeat under AES-GCM
exposes plaintext and lets an attacker forge frames. A 1,000-node cluster
sending about 17 datagrams per node-second reaches 2^32 in roughly three
days. Rotate the primary key well inside that bound. There is no replay
protection beyond the protocol's own incarnation and sequence checks: a
captured frame stays valid under its key and can be sent again.

Identity/address/count checks, bounded frame allocation, stream deadlines
and finite driver queues limit input handling. CIDR admission
(`Protocol.Allowed`) applies to network sources and advertised membership
addresses. These controls do not replace network isolation, secure key
distribution, or application authorization. See the README for the wire
limits and notification contract.
