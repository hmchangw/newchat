// Package auditarchive is the on-the-wire and at-rest format shared by
// archive-worker (writer) and audit-service (reader): the archive record and
// its hash, the frame and blob ciphers, the sealed segment layout and object
// keys, and the Elasticsearch documents, templates and lifecycle policy.
//
// Nothing here talks to NATS, Vault, a bucket or a cluster. It is pure data
// so both sides can be tested against the same bytes.
package auditarchive
