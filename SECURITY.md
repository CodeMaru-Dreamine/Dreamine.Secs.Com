# Security policy

Report suspected vulnerabilities privately to the repository maintainers; do not include credentials, proprietary protocol captures, customer identifiers, or licensed standards text in an issue.

The runtime treats peer frames as untrusted. Frame/message size, item depth, list count, length fields, protocol types, session IDs, and transaction correlation are validated before application dispatch. Configure `MaximumFrameLength` for the deployment and use network segmentation/TLS termination appropriate to the facility; HSMS itself does not provide authentication or encryption.

Supported security fixes target the latest source revision. This repository makes no promise that an unpublished package has a public servicing channel.
